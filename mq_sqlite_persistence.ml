
open Lwt
open Printf
open Mq_types
open Sqlexpr_sqlite

module MSET = Set.Make(struct type t = int * message let compare = compare end)
module SSET = Set.Make(String)

type t = {
  db : db;
  in_mem : (string, MSET.t) Hashtbl.t;
  in_mem_msgs : (string, message) Hashtbl.t;
  mutable ack_pending : SSET.t;
}

let flush t =
  transaction t.db begin fun db ->
    printf "Flushing %d messages to disk (%d ACK pending)\n%!"
      (Hashtbl.length t.in_mem_msgs) (SSET.cardinal t.ack_pending);
    Hashtbl.iter
      (fun _ msg ->
         execute db
           sqlc"INSERT INTO ocamlmq_msgs
                 (msg_id, priority, destination, timestamp,
                  ack_timeout, body)
               VALUES(%s, %d, %s, %f, %f, %S)"
           msg.msg_id msg.msg_priority (destination_name msg.msg_destination)
           msg.msg_timestamp msg.msg_ack_timeout msg.msg_body)
      t.in_mem_msgs;
    SSET.iter
      (execute db sqlc"INSERT INTO pending_acks(msg_id) VALUES(%s)")
      t.ack_pending;
    Hashtbl.clear t.in_mem;
    Hashtbl.clear t.in_mem_msgs;
    t.ack_pending <- SSET.empty;
  end

let make file =
  let t =
    { db = open_db file; in_mem = Hashtbl.create 13;
      in_mem_msgs = Hashtbl.create 13; ack_pending = SSET.empty; } in
  let rec loop_flush () = Lwt_unix.sleep 1.0 >> (flush t; loop_flush ()) in
    Lwt.ignore_result
      (try_lwt loop_flush ()
       with e -> printf "EXCEPTION IN FLUSHER: %s\n%!" (Printexc.to_string e);
                 return ());
    t

let initialize t =
  execute t.db sql"ATTACH \":memory:\" AS mem";
  execute t.db
    sql"CREATE TABLE IF NOT EXISTS ocamlmq_msgs(
          msg_id VARCHAR(255) NOT NULL PRIMARY KEY,
          priority INT NOT NULL,
          destination VARCHAR(255) NOT NULL,
          timestamp DOUBLE NOT NULL,
          ack_timeout DOUBLE NOT NULL,
          body BLOB NOT NULL)";
  execute t.db
    sql"CREATE INDEX IF NOT EXISTS
        ocamlmq_msgs_destination_priority_timestamp
        ON ocamlmq_msgs(destination, priority, timestamp)";
  execute t.db
    sql"CREATE TABLE mem.pending_acks(msg_id VARCHAR(255) NOT NULL PRIMARY KEY)";
  return ()

let save_msg t ?low_priority msg =
  let dest = destination_name msg.msg_destination in
  let v = (msg.msg_priority, msg) in
  begin
    try Hashtbl.replace t.in_mem dest (MSET.add v (Hashtbl.find t.in_mem dest));
    with Not_found -> Hashtbl.add t.in_mem dest (MSET.singleton v )
  end;
  Hashtbl.add t.in_mem_msgs msg.msg_id msg;
  return ()

let register_ack_pending_new_msg t msg =
  t.ack_pending <- SSET.add msg.msg_id t.ack_pending;
  save_msg t msg

let register_ack_pending_msg t msg_id =
  if Hashtbl.mem t.in_mem_msgs msg_id then
    let r = SSET.mem msg_id t.ack_pending in
      if not r then t.ack_pending <- SSET.add msg_id t.ack_pending;
      return (not r)
  else
    match select t.db sqlc"SELECT @s{msg_id} FROM mem WHERE msg_id = %s" msg_id with
        [x] -> return false
      | _ ->
          execute t.db sqlc"INSERT INTO pending_acks(msg_id) VALUES(%s)" msg_id;
          return true

let msg_of_tuple (msg_id, dst, timestamp, priority, ack_timeout, body) =
  {
    msg_id = msg_id;
    msg_destination = Queue dst;
    msg_priority = priority;
    msg_timestamp = timestamp;
    msg_ack_timeout = ack_timeout;
    msg_body = body;
  }

let get_ack_pending_msg t msg_id =
  try
    let msg = Hashtbl.find t.in_mem_msgs msg_id in
      return (if SSET.mem msg_id t.ack_pending then Some msg else None)
  with Not_found ->
    match
      select t.db
        sqlc"SELECT @s{msg_id}, @s{destination}, @f{timestamp},
                   @d{priority}, @f{ack_timeout}, @S{body}
              FROM ocamlmq_msgs AS msg
             WHERE msg_id = %s
               AND EXISTS (SELECT 1 FROM pending_acks WHERE msg_id = msg.msg_id)"
        msg_id
    with
        [] -> return None
      | msg :: _ -> return (Some (msg_of_tuple msg))

let ack_msg t msg_id =
  if SSET.mem msg_id t.ack_pending then begin
    let msg = Hashtbl.find t.in_mem_msgs msg_id in
      Hashtbl.remove t.in_mem_msgs msg_id;
      t.ack_pending <- SSET.remove msg_id t.ack_pending;
      let v = (msg.msg_priority, msg) in
      let dst = destination_name msg.msg_destination in
        Hashtbl.replace t.in_mem dst (MSET.remove v (Hashtbl.find t.in_mem dst))
  end else begin
    execute t.db sqlc"DELETE FROM ocamlmq_msgs WHERE msg_id = %s" msg_id;
    execute t.db sqlc"DELETE FROM pending_acks WHERE msg_id = %s" msg_id;
  end;
  return ()

let unack_msg t msg_id =
  if SSET.mem msg_id t.ack_pending then
    t.ack_pending <- SSET.remove msg_id t.ack_pending
  else
    execute t.db sqlc"DELETE FROM pending_acks WHERE msg_id = %s" msg_id;
  return ()

exception Msg of message

let get_msg_for_delivery t dest =
  try
    let set = Hashtbl.find t.in_mem dest in
      try
        MSET.iter
          (fun (_, msg) ->
             if not (SSET.mem msg.msg_id t.ack_pending) then raise (Msg msg))
          set;
        raise Not_found
      with Msg msg ->
        t.ack_pending <- SSET.add msg.msg_id t.ack_pending;
        return (Some msg)
  with Not_found ->
    let tup =
      select t.db
        sqlc"SELECT @s{msg_id}, @s{destination}, @f{timestamp},
                   @d{priority}, @f{ack_timeout}, @S{body}
              FROM ocamlmq_msgs as msg
             WHERE destination = %s
               AND NOT EXISTS (SELECT 1 FROM pending_acks WHERE msg_id = msg.msg_id)
          ORDER BY priority, timestamp
             LIMIT 1 "
        dest
    in match tup with
        [] -> return None
      | tup :: _ ->
          let msg = msg_of_tuple tup in
          execute t.db sqlc"INSERT INTO pending_acks VALUES(%s)" msg.msg_id;
          return (Some msg)

let count_queue_msgs t dest =
  let in_mem =
    try MSET.cardinal (Hashtbl.find t.in_mem dest) with Not_found -> 0 in
  let in_db =
    match
      select t.db sqlc"SELECT @L{COUNT(*)} FROM ocamlmq_msgs WHERE destination=%s"
        dest
    with [n] -> n
      | _ -> assert false
  in return (Int64.add (Int64.of_int in_mem) in_db)

let crash_recovery t = return ()