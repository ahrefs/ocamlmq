open Lwt
open Mq_types

module Option  = BatOption

module Sqlexpr = Sqlexpr_sqlite.Make(Sqlexpr_concurrency.Id)
open Sqlexpr

module MSET = Set.Make(struct
                         type t = int * message
                         let compare ((p1, m1) : t) (p2, m2) =
                           if p2 = p1 then
                             String.compare m1.msg_id m2.msg_id
                           else p2 - p1
                       end)
module SSET = Set.Make(String)

(* "timestamp-ordered set": ordering on 1. priority, 2. timestamp, 3. msg_id *)
module TSET = Set.Make
  (struct
     type t = message
     let compare (m1 : t) m2 =
       match Pervasives.compare m1.msg_priority m2.msg_priority with
       | r when r < 0 -> -1
       | 0 -> begin
           match Pervasives.compare m1.msg_timestamp m2.msg_timestamp with
           | r when r < 0 -> -1
           | 0 -> String.compare m1.msg_id m2.msg_id
           | _ (* r > 0 *) -> 1
         end
       | _ (* r > 0 *) -> 1
   end)

type t = {
  db : Sqlexpr.db;
  in_mem : (string, MSET.t * SSET.t * TSET.t) Hashtbl.t;
  in_mem_msgs : (string, message) Hashtbl.t;
  mutable ack_pending : SSET.t;
  mutable flush_alarm : unit Lwt.u;
  max_msgs_in_mem : int;
  mutable unacks : SSET.t;
  binlog_file : string option;
  mutable binlog : Binlog.t option;
  sync_binlog : bool;
  mutable shutdown_flushers : bool;
  debug : bool;
  ordering : bool;
  with_lock : 'a . (unit -> 'a Lwt.t) -> 'a Lwt.t
}

let tset_add t msg s =
  if t.ordering
  then TSET.add msg s
  else s

let tset_singleton t msg =
  if t.ordering
  then TSET.singleton msg
  else TSET.empty

let tset_remove t msg s =
  if t.ordering
  then TSET.remove msg s
  else s

let count_unmaterialized_pending_acks db =
  select_one db sqlc"SELECT @L{COUNT(*)} FROM pending_acks"

let count_acked_messages db =
  select_one db sqlc"SELECT @L{COUNT(*)} FROM acked_msgs"

let pr fmt = Printf.(ksprintf (eprintf "%s%!") fmt)
let puts fmt = Printf.(ksprintf prerr_endline fmt)

let timed verbose gen_msg f =
  let t0 = Unix.gettimeofday () in
    if verbose then pr "%s" (gen_msg ());
    let y = f () in
      if verbose then puts " (%8.5fs)" (Unix.gettimeofday () -. t0);
      y

let flush_acked_msgs ?(verbose = false) db =
  timed verbose begin fun () ->
      Printf.sprintf "Flushing %Ld ACKs" (count_acked_messages db)
    end
    (fun () ->
       execute db
         sqlc"DELETE FROM ocamlmq_msgs WHERE msg_id IN (SELECT * FROM acked_msgs)";
       execute db sqlc"DELETE FROM acked_msgs")

let materialize_pending_acks ?(verbose = false) db =
  timed verbose
    (fun () -> Printf.sprintf "Materializing %Ld pending ACKs in DB"
                 (count_unmaterialized_pending_acks db))
    (fun () ->
       execute db sqlc"UPDATE ocamlmq_msgs SET ack_pending = 1
                        WHERE msg_id IN (SELECT msg_id FROM pending_acks)";
       execute db sqlc"DELETE FROM pending_acks")

let rec flush t =
  let t0 = Unix.gettimeofday () in
  let flushed = ref false in
  transaction t.db begin fun db ->
    let in_mem_msgs = Hashtbl.length t.in_mem_msgs
    and ack_pending = SSET.cardinal t.ack_pending
    and unmaterialized_ack_pendings = count_unmaterialized_pending_acks db
    and acked_msgs = count_acked_messages db in
      if in_mem_msgs <> 0 || ack_pending <> 0 ||
         unmaterialized_ack_pendings <> 0L || acked_msgs <> 0L
      then begin
        flushed := true;
        if t.debug then
          pr "Flushing to disk: %d msgs, %d + %Ld pending ACKS, %Ld ACKS"
            in_mem_msgs ack_pending unmaterialized_ack_pendings acked_msgs;
        do_flush t db;
      end
  end;
  Option.may Binlog.truncate t.binlog;
  if !flushed then
    if t.debug then puts " (%8.5fs)" (Unix.gettimeofday () -. t0)

and do_flush t db =
  Hashtbl.iter
    (fun _ msg ->
       execute db
         sqlc"INSERT OR IGNORE INTO ocamlmq_msgs
               (ack_pending, msg_id, priority, destination, timestamp,
                ack_timeout, body, headers)
             VALUES(0, %s, %d, %s, %f, %f, %S, %S)"
         msg.msg_id msg.msg_priority (destination_name msg.msg_destination)
         msg.msg_timestamp msg.msg_ack_timeout msg.msg_body
         (Mq_types.serialize_headers msg.msg_headers)
    )
    t.in_mem_msgs;
  SSET.iter
    (execute db sqlc"INSERT INTO pending_acks(msg_id) VALUES(%s)")
    t.ack_pending;
  flush_acked_msgs db;
  materialize_pending_acks db;
  Hashtbl.clear t.in_mem;
  Hashtbl.clear t.in_mem_msgs;
  t.ack_pending <- SSET.empty

let msg_materialized_as_ack_pending db msg_id =
  select_one db
    sqlc"SELECT @b{EXISTS (SELECT 1 FROM ocamlmq_msgs
                            WHERE msg_id = %s AND ack_pending)}"
    msg_id

let unack db msg_id =
  execute db sqlc"DELETE FROM pending_acks WHERE msg_id = %s" msg_id;
  if msg_materialized_as_ack_pending db msg_id then
    execute db
      sqlc"UPDATE ocamlmq_msgs SET ack_pending = 0 WHERE msg_id = %s"
      msg_id

let check_sqlite_version_ok db =
  let v = select_one db sqlc"SELECT @s{sqlite_version()}" in
  let to_num s = Scanf.sscanf s "%d" (fun n -> n) in
  let ns = List.map to_num (BatString.nsplit v ~by:".") in
    if ns < [ 3; 6; 8 ] then
      failwith (Printf.sprintf "Need sqlite3 >= 3.6.8 (found: %s)" v)

let make ?(max_msgs_in_mem = max_int) ?(flush_period = 1.0)
         ?binlog ?(sync_binlog = false) ?(debug = false) ?(ordering = false)
         file =
  let wait_flush, awaken_flush = Lwt.wait () in
  let mut = Lwt_mutex.create () in
  let t =
    { db = Sqlexpr.open_db file; in_mem = Hashtbl.create 13;
      in_mem_msgs = Hashtbl.create 13; ack_pending = SSET.empty;
      flush_alarm = awaken_flush;
      max_msgs_in_mem = max_msgs_in_mem;
      unacks = SSET.empty;
      binlog_file = binlog; binlog = None;
      sync_binlog = sync_binlog;
      shutdown_flushers = false;
      debug;
      ordering = ordering;
      with_lock = (fun f -> Lwt_mutex.with_lock mut f);
    } in
  let flush_period = max flush_period 0.005 in
  let rec loop_flush wait_flush =
    Lwt.pick [Lwt_unix.sleep flush_period; wait_flush] >>
    if t.shutdown_flushers
    then
      return ()
    else begin
      let wait, awaken = Lwt.wait () in
        lwt () = t.with_lock begin fun () ->
          flush t;
          return ()
        end in
        t.flush_alarm <- awaken;
        loop_flush wait
    end in
  let rec loop_flush_unacks () =
    lwt () = Lwt_unix.sleep 0.1 in
    if t.shutdown_flushers
    then
      return ()
    else begin
      if not (SSET.is_empty t.unacks) then
        transaction t.db
          (fun db ->
             if t.debug then
               puts "UnACKing %d messages in DB" (SSET.cardinal t.unacks);
             SSET.iter (unack db) t.unacks);
      t.unacks <- SSET.empty;
      loop_flush_unacks ()
    end
  in
    check_sqlite_version_ok t.db;
    ignore
      (try_lwt loop_flush wait_flush
       with e ->
         if t.debug then
           puts "EXCEPTION IN FLUSHER: %s" (Printexc.to_string e);
         return ()
      );
    ignore
      (try_lwt loop_flush_unacks ()
       with e ->
         if t.debug then
           puts "EXCEPTION IN UNACK FLUSHER: %s" (Printexc.to_string e);
         return ()
      );
    t

let initialize t =
  execute t.db sqlinit"ATTACH \":memory:\" AS mem";
  execute t.db
    sqlinit"CREATE TABLE IF NOT EXISTS ocamlmq_msgs(
              ack_pending BOOL NOT NULL,
              msg_id VARCHAR(255) NOT NULL PRIMARY KEY,
              priority INT NOT NULL,
              destination VARCHAR(255) NOT NULL,
              timestamp DOUBLE NOT NULL,
              ack_timeout DOUBLE NOT NULL,
              body BLOB NOT NULL,
              headers BLOB NOT NULL
            )";
  execute t.db
    sqlinit"CREATE INDEX IF NOT EXISTS
            ocamlmq_msgs_destination_priority_timestamp
            ON ocamlmq_msgs(destination, ack_pending, priority, timestamp)";
  execute t.db
    sqlinit"CREATE INDEX IF NOT EXISTS
            ocamlmq_msgs_ack_pending
            ON ocamlmq_msgs(ack_pending)";
  execute t.db
    sqlinit"CREATE TABLE mem.pending_acks(msg_id VARCHAR(255) NOT NULL PRIMARY KEY)";
  execute t.db
    sqlinit"CREATE TABLE mem.acked_msgs(msg_id VARCHAR(255) NOT NULL PRIMARY KEY)";
  execute t.db
    sqlinit"PRAGMA journal_mode=WAL;";
  (* execute t.db *)
    (* sqlinit"PRAGMA synchronous=NORMAL;"; *)
  return ()

let do_save_msg ?(can_flush = true) t sent msg =
  let dest = destination_name msg.msg_destination in
  let v = (msg.msg_priority, msg) in
  begin
    try
      let unsent, want_ack, unsent_ord = Hashtbl.find t.in_mem dest in
      let p =
        if sent then (unsent, SSET.add msg.msg_id want_ack, unsent_ord)
        else (MSET.add v unsent, want_ack, tset_add t msg unsent_ord)
      in Hashtbl.replace t.in_mem dest p
    with Not_found ->
      let p =
        if sent then (MSET.empty, SSET.singleton msg.msg_id, TSET.empty)
        else (MSET.singleton v, SSET.empty, tset_singleton t msg)
      in Hashtbl.add t.in_mem dest p
  end;
  Hashtbl.add t.in_mem_msgs msg.msg_id msg;
  if can_flush && Hashtbl.length t.in_mem_msgs > t.max_msgs_in_mem then
    Lwt.wakeup t.flush_alarm ();
  return ()

let save_msg t ?low_priority msg =
  (* Locking is needed as there happens following scenario:
     1. save_msg is called from [Mq_server.cmd_send],
     2. message is stored to binlog (but not to t.in_mem_msgs),
     3. periodical flush awakes on timer, flushes messages from t.in_mem_msgs
        (without the message passed to save_msg, see next step) and truncates
        binlog,
     4. message is stored to t.in_mem_msgs,
     5. program is killed.
     So the message is not flushed to persistent storage and also is absent in
     binlog.
     [save_msg] and [loop_flush] lock same mutex (using [with_lock]) to avoid
     this scenario.
   *)
  t.with_lock begin fun () ->
    ignore low_priority;
    Option.map_default (fun log -> Binlog.add log msg) (return ()) t.binlog >>
    do_save_msg t false msg
  end

let register_ack_pending_msg t msg_id =
  if Hashtbl.mem t.in_mem_msgs msg_id then
    let r = SSET.mem msg_id t.ack_pending in
      if not r then begin
        let msg = Hashtbl.find t.in_mem_msgs msg_id in
        let dest = destination_name msg.msg_destination in
        let unsent, want_ack, unsent_ord = Hashtbl.find t.in_mem dest in
        let v = (msg.msg_priority, msg) in
          t.ack_pending <- SSET.add msg_id t.ack_pending;
          Hashtbl.replace t.in_mem dest
            (MSET.remove v unsent, SSET.add msg_id want_ack,
             tset_remove t msg unsent_ord)
      end;
      return (not r)
  else
    match
      select_one_maybe t.db
        sqlc"SELECT @b{1} FROM pending_acks WHERE msg_id = %s" msg_id
    with
        Some _ -> return false
      | None ->
          if msg_materialized_as_ack_pending t.db msg_id then
            return false
          else begin
            execute t.db sqlc"INSERT INTO pending_acks(msg_id) VALUES(%s)" msg_id;
            if count_unmaterialized_pending_acks t.db > 100L then
              transaction t.db (materialize_pending_acks ~verbose:true);
            return true
          end

let msg_of_tuple
  (msg_id, dst, timestamp, priority, ack_timeout, body, headers_ser) =
  {
    msg_id = msg_id;
    msg_destination = Queue dst;
    msg_priority = priority;
    msg_timestamp = timestamp;
    msg_ack_timeout = ack_timeout;
    msg_body = body;
    msg_headers = Mq_types.deserialize_headers headers_ser;
  }

let get_ack_pending_msg t msg_id =
  return begin try
    let msg = Hashtbl.find t.in_mem_msgs msg_id in
      if SSET.mem msg_id t.ack_pending then Some msg else None
  with Not_found ->
    select_one_f_maybe t.db msg_of_tuple
      sqlc"SELECT @s{msg_id}, @s{destination}, @f{timestamp},
                 @d{priority}, @f{ack_timeout}, @S{body}, @S{headers}
            FROM ocamlmq_msgs AS msg
           WHERE msg_id = %s
             AND (msg.ack_pending OR
                  EXISTS (SELECT 1 FROM pending_acks WHERE msg_id = msg.msg_id))
             AND NOT EXISTS (SELECT 1 FROM acked_msgs WHERE msg_id = msg.msg_id)"
      msg_id
  end

let do_ack_msg ~can_flush t msg_id =
  if SSET.mem msg_id t.ack_pending then begin
    let msg = Hashtbl.find t.in_mem_msgs msg_id in
      Hashtbl.remove t.in_mem_msgs msg_id;
      t.ack_pending <- SSET.remove msg_id t.ack_pending;
      let dst = destination_name msg.msg_destination in
      let unsent, want_ack, unsent_ord = Hashtbl.find t.in_mem dst in
        Hashtbl.replace t.in_mem dst
          (unsent, SSET.remove msg_id want_ack, unsent_ord)
  end else begin
    execute t.db sqlc"INSERT INTO acked_msgs(msg_id) VALUES(%s)" msg_id;
    t.ack_pending <- SSET.remove msg_id t.ack_pending;
    if can_flush && count_acked_messages t.db > 100L then
      transaction t.db (flush_acked_msgs ~verbose:true)
  end

let ack_msg t msg_id =
  t.with_lock begin fun () ->
    lwt () = Option.map_default
      (fun log -> Binlog.cancel log msg_id) (return ()) t.binlog in
    do_ack_msg ~can_flush:true t msg_id;
    return ()
  end

let unack_msg t msg_id =
  if SSET.mem msg_id t.ack_pending then begin
    t.ack_pending <- SSET.remove msg_id t.ack_pending;
    let msg = Hashtbl.find t.in_mem_msgs msg_id in
    let dst = destination_name msg.msg_destination in
    let msg = Hashtbl.find t.in_mem_msgs msg_id in
    let unsent, want_ack, unsent_ord = Hashtbl.find t.in_mem dst in
    let v = (msg.msg_priority, msg) in
      Hashtbl.replace t.in_mem dst
        (MSET.add v unsent, SSET.remove msg_id want_ack,
         tset_add t msg unsent_ord)
  end else begin
    t.unacks <- SSET.add msg_id t.unacks;
  end;
  return ()

let get_msg_from_mem_no_order t dest =
  match BatHashtbl.find_option t.in_mem dest with
  | None -> None
  | Some (unsent, want_ack, unsent_ord) ->
      match try Some (MSET.min_elt unsent) with Not_found -> None with
      | None -> None
      | Some ((_prio, msg) as v) ->
          t.ack_pending <- SSET.add msg.msg_id t.ack_pending;
          Hashtbl.replace t.in_mem dest
            (MSET.remove v unsent, SSET.add msg.msg_id want_ack,
             tset_remove t msg unsent_ord);
          Some msg

let get_msg_from_mem_order t dest =
  match BatHashtbl.find_option t.in_mem dest with
  | None -> None
  | Some (unsent, want_ack, unsent_ord) ->
      match try Some (TSET.min_elt unsent_ord) with Not_found -> None with
      | None -> None
      | Some msg ->
          t.ack_pending <- SSET.add msg.msg_id t.ack_pending;
          let v = (msg.msg_priority, msg) in
          Hashtbl.replace t.in_mem dest
            (MSET.remove v unsent, SSET.add msg.msg_id want_ack,
             tset_remove t msg unsent_ord);
          Some msg

let get_msg_from_db t dest =
  match
    select_one_f_maybe t.db msg_of_tuple
      sqlc"SELECT @s{msg_id}, @s{destination}, @f{timestamp},
                 @d{priority}, @f{ack_timeout}, @S{body}, @S{headers}
            FROM ocamlmq_msgs as msg
           WHERE destination = %s
             AND msg.ack_pending = 0
             AND NOT EXISTS (SELECT 1 FROM pending_acks WHERE msg_id = msg.msg_id)
             AND NOT EXISTS (SELECT 1 FROM acked_msgs WHERE msg_id = msg.msg_id)
        ORDER BY priority, timestamp"
      dest
  with
      None -> None
    | Some msg ->
        execute t.db sqlc"INSERT INTO pending_acks VALUES(%s)" msg.msg_id;
        if count_unmaterialized_pending_acks t.db > 100L then
          transaction t.db (materialize_pending_acks ~verbose:true);
        Some msg

let get_msg_for_delivery t dest =
  return begin
    if t.ordering
    then begin
      match get_msg_from_db t dest with
      | (Some _msg) as some_msg -> some_msg
      | None -> get_msg_from_mem_order t dest
    end else begin
      match get_msg_from_mem_no_order t dest with
      | (Some _msg) as some_msg -> some_msg
      | None -> get_msg_from_db t dest
    end
  end

let count_queue_msgs t dst =
  let in_mem =
    try
      let unsent, want_ack, _unsent_ord = Hashtbl.find t.in_mem dst in
        MSET.cardinal unsent + SSET.cardinal want_ack
    with Not_found -> 0 in
  let in_db =
    select_one t.db
      sqlc"SELECT @L{COUNT(*)} FROM ocamlmq_msgs as msg
            WHERE destination=%s
              AND NOT EXISTS (SELECT 1 FROM acked_msgs WHERE msg_id = msg.msg_id)"
      dst
  in
    return (Int64.add (Int64.of_int in_mem) in_db)

let crash_recovery t =
  execute t.db sqlc"UPDATE ocamlmq_msgs SET ack_pending = 0 WHERE ack_pending = 1";
  begin match t.binlog_file with
      None -> return ()
    | Some f ->
        lwt binlog, added_msgs, acked_msg_ids =
          Binlog.make ~sync:t.sync_binlog f
        in
          t.binlog <- Some binlog;
          if t.debug then
            Printf.eprintf "(binlog: %d msgs added, %d msgs acked) %!"
              (List.length added_msgs) (List.length acked_msg_ids);
          lwt () = Lwt_list.iter_s (do_save_msg ~can_flush:false t false)
            added_msgs in
          List.iter (do_ack_msg ~can_flush:false t) acked_msg_ids;
          flush t;
          return ()
  end

let shutdown t =
  t.shutdown_flushers <- true;
  flush t;
  Sqlexpr.close_db t.db;
  return_unit

let init_db, _check_db, auto_check_db = sql_check"sqlite"
