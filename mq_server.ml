open Printf
open Lwt

module type PERSISTENCE =
sig
  type t

  val save_msg : t -> Mq_types.message -> unit Lwt.t
  val register_ack_pending_new_msg : t -> Mq_types.message -> unit Lwt.t
  val register_ack_pending_msg : t -> string -> unit Lwt.t
  val get_ack_pending_msg : t -> string -> Mq_types.message option Lwt.t
  val ack_msg : t -> string -> unit Lwt.t
  val unack_msg : t -> string -> unit Lwt.t
  val get_queue_msgs : t -> string -> int -> Mq_types.message list Lwt.t
  val crash_recovery : t -> unit Lwt.t
end

module Make(P : PERSISTENCE) =
struct

open Mq_types
module STOMP = Mq_stomp
module ACKS = Set.Make(struct
                         type t = string * unit Lwt.t * unit Lwt.u
                         let compare (s1, _, _) (s2, _, _) = String.compare s1 s2
                       end)

type message_kind = New | Old | Requeued

type subscription = {
  qs_name : string;
  qs_prefetch : int;
  mutable qs_pending_acks : ACKS.t;
}

let dummy_subscription =
  { qs_name = ""; qs_prefetch = 0; qs_pending_acks = ACKS.empty }

type connection = {
  conn_id : int;
  conn_ich : Lwt_io.input_channel;
  conn_och : Lwt_io.output_channel;
  conn_default_prefetch : int;
  mutable conn_pending_acks : int;
  conn_queues : (string, subscription) Hashtbl.t;
  conn_topics : (string, subscription) Hashtbl.t;
}

module CONNS = Set.Make(struct
                          type t = connection
                          let compare t1 t2 = t2.conn_id - t1.conn_id
                        end)

module SUBS = ExtSet.Make(struct
                            type t = (connection * subscription)
                            let compare (t1, _) (t2, _) = t2.conn_id - t1.conn_id
                          end)

type listeners = {
  mutable l_ready : SUBS.t;
  mutable l_blocked : SUBS.t;
  mutable l_last_sent : SUBS.elt option;
}

type broker = {
  mutable b_connections : CONNS.t;
  b_queues : (string, listeners) Hashtbl.t;
  b_topics : (string, CONNS.t ref) Hashtbl.t;
  b_socket : Lwt_unix.file_descr;
  b_frame_eol : bool;
  b_msg_store : P.t;
}

let send_error broker conn fmt =
  STOMP.send_error ~eol:broker.b_frame_eol conn.conn_och fmt

let send_to_topic broker name msg =
  try
    let s = Hashtbl.find broker.b_topics name in
      Lwt.ignore_result
        (Lwt_util.iter
           (fun conn -> STOMP.send_message ~eol:broker.b_frame_eol conn.conn_och msg)
           (CONNS.elements !s));
      return ()
  with Not_found -> return ()

let subs_wanted_msgs subs =
  if subs.qs_prefetch <= 0 then max_int
  else subs.qs_prefetch - ACKS.cardinal subs.qs_pending_acks

let is_subs_blocked subs =
  subs.qs_prefetch >= 0 && ACKS.cardinal subs.qs_pending_acks >= subs.qs_prefetch

let select_blocked_subs s = SUBS.filter (fun (_, x) -> is_subs_blocked x) s

let select_unblocked_subs s = SUBS.filter (fun (_, x) -> not (is_subs_blocked x)) s

let block_subscription listeners ((conn, subs) as c) =
  listeners.l_ready <- SUBS.remove c listeners.l_ready;
  listeners.l_blocked <- SUBS.add c listeners.l_blocked

let unblock_some_listeners listeners =
  let unblocked = select_unblocked_subs listeners.l_blocked in
    listeners.l_ready <- SUBS.union listeners.l_ready unblocked;
    listeners.l_blocked <- SUBS.diff listeners.l_blocked unblocked

let find_recipient broker name =
  try
    let ls = Hashtbl.find broker.b_queues name in
      match ls.l_last_sent with
          None -> (* first msg sent, there can be no blocked client *)
            Some (ls, SUBS.min_elt ls.l_ready)
        | Some cursor ->
            if SUBS.is_empty ls.l_ready then unblock_some_listeners ls;
            match SUBS.next cursor ls.l_ready with
              | (conn, _) when conn == fst (SUBS.min_elt ls.l_ready) ->
                  (* went through all ready subscriptions, try to unblock some &
                   * give it another try *)
                  unblock_some_listeners ls;
                  Some (ls, SUBS.next cursor ls.l_ready)
              | c -> Some (ls, c)
  with Not_found -> None

let send_to_recipient ~kind broker listeners conn subs msg =
  print_endline "send_to_recipient";
  let sleep, wakeup = Lwt.task () in
  let msg_id = msg.msg_id in
    subs.qs_pending_acks <- ACKS.add (msg.msg_id, sleep, wakeup)
                                     subs.qs_pending_acks;
    listeners.l_last_sent <- Some (conn, subs);
    if is_subs_blocked subs then block_subscription listeners (conn, subs);
    (match kind with
         Requeued -> (* the message was already in ACK-pending set *) return ()
       | New -> P.register_ack_pending_new_msg broker.b_msg_store msg
       | Old -> (* just move to ACK *)
           P.register_ack_pending_msg broker.b_msg_store msg_id) >>
    STOMP.send_message ~eol:broker.b_frame_eol conn.conn_och msg >>
    let threads = match msg.msg_ack_timeout with
        dt when dt > 0. -> [ Lwt_unix.timeout dt; sleep ]
      | _ -> [ sleep ] in
    Lwt.select threads >>
    P.ack_msg broker.b_msg_store msg_id

let rec enqueue_after_timeout broker msg_id =
  P.get_ack_pending_msg broker.b_msg_store msg_id >>= function
      None -> return ()
    | Some msg ->
        let queue = destination_name msg.msg_destination in
        match find_recipient broker queue with
          | None -> begin (* move to main table *)
              eprintf "No recipient for non-ACK message, saving.\n%!";
              P.unack_msg broker.b_msg_store msg_id
            end
          | Some (listeners, (conn, subs)) ->
              eprintf "Found a recipient for this message, sending\n%!";
              try_lwt
                send_to_recipient ~kind:Requeued broker listeners conn subs msg
              with Lwt_unix.Timeout | Lwt.Canceled ->
                eprintf "enqueue_after_timeout: TIMEOUT or CANCELED!\n%!";
                enqueue_after_timeout broker msg_id

let rec send_to_queue broker queue msg = match find_recipient broker queue with
    None -> P.save_msg broker.b_msg_store msg
  | Some (listeners, (conn, subs)) ->
      let msg_id = msg.msg_id in
      try_lwt
        send_to_recipient ~kind:New broker listeners conn subs msg
      with Lwt_unix.Timeout | Lwt.Canceled ->
        eprintf "send_to_queue: TIMEOUT or CANCELED\n%!";
        enqueue_after_timeout broker msg_id

let rec send_saved_messages broker listeners (conn, subs) =

  let do_send msg =
    let msg_id = msg.msg_id in
    if not (is_subs_blocked subs) then
      try_lwt
        send_to_recipient ~kind:Old broker listeners conn subs msg
      with Lwt_unix.Timeout | Lwt.Canceled ->
        eprintf "send_saved_messages: TIMEOUT or CANCELED\n%!";
        enqueue_after_timeout broker msg_id
    else return () in

  let wanted = subs_wanted_msgs subs in
    P.get_queue_msgs broker.b_msg_store subs.qs_name wanted >>=
      Lwt_util.iter do_send

let send_message broker msg = match msg.msg_destination with
    Queue name -> send_to_queue broker name msg
  | Topic name -> send_to_topic broker name msg

let new_id prefix =
  let cnt = ref 0 in
    fun () ->
      incr cnt;
      sprintf "%s-%f-%d" prefix (Unix.gettimeofday ()) !cnt

let new_msg_id = new_id "msg"

let new_conn_id = let n = ref 0 in fun () -> incr n; !n

let cmd_subscribe broker conn frame =
  try_lwt
    let destination = STOMP.get_destination frame in
    let subscription =
      {
        qs_name = (match destination with Topic n | Queue n -> n);
        qs_prefetch =
          (try
             int_of_string (STOMP.get_header frame "prefetch")
           with _ -> -1);
        qs_pending_acks = ACKS.empty;
      }
    in match destination with
        Topic name -> begin
          Hashtbl.replace conn.conn_topics name subscription;
          try
            let s = Hashtbl.find broker.b_topics name in
              s := CONNS.add conn !s;
              return ()
          with Not_found ->
            Hashtbl.add broker.b_topics name (ref (CONNS.singleton conn));
            return ()
        end
      | Queue name -> begin
          Hashtbl.replace conn.conn_queues name subscription;
          let listeners =
            try
              let ls = Hashtbl.find broker.b_queues name in
                ls.l_ready <- SUBS.add (conn, subscription) ls.l_ready;
                ls
            with Not_found ->
              let ls = { l_ready = SUBS.singleton (conn, subscription);
                         l_blocked = SUBS.empty;
                         l_last_sent = None }
              in Hashtbl.add broker.b_queues name ls;
                 ls
          in Lwt.ignore_result
               (send_saved_messages broker listeners (conn, subscription));
             return ()
        end
  with Not_found ->
    STOMP.send_error ~eol:broker.b_frame_eol conn.conn_och
      "Invalid or missing destination: must be of the form /queue/xxx or /topic/xxx."

let cmd_unsubscribe broker conn frame =
  try
    match STOMP.get_destination frame with
        Topic name -> begin
          try
            let s = Hashtbl.find broker.b_topics name in
              s := CONNS.remove conn !s;
              if CONNS.is_empty !s then
                Hashtbl.remove broker.b_topics name;
              return ()
          with Not_found -> return ()
        end
      | Queue name -> begin
          try
            let ls = Hashtbl.find broker.b_queues name in
              ls.l_ready <- SUBS.remove (conn, dummy_subscription) ls.l_ready;
              ls.l_blocked <- SUBS.remove (conn, dummy_subscription) ls.l_blocked;
              if SUBS.is_empty ls.l_ready && SUBS.is_empty ls.l_blocked then
                Hashtbl.remove broker.b_queues name;
              return ()
          with Not_found -> return ()
        end
  with Not_found ->
    STOMP.send_error ~eol:broker.b_frame_eol conn.conn_och
      "Invalid or missing destination: must be of the form /queue/xxx or /topic/xxx."

let cmd_disconnect broker conn frame =
  Lwt_io.abort conn.conn_och >>
  Lwt_io.abort conn.conn_ich >>
  fail End_of_file

let cmd_send broker conn frame =
  try_lwt
    send_message broker
      {
        msg_id = sprintf "conn-%d:%s" conn.conn_id (new_msg_id ());
        msg_destination = STOMP.get_destination frame;
        msg_priority = 0;
        msg_timestamp = Unix.gettimeofday ();
        msg_body = frame.STOMP.fr_body;
        msg_ack_timeout =
          (try
             float_of_string (STOMP.get_header frame "ack-timeout")
           with _ -> 0.)
      }
  with Not_found ->
    STOMP.send_error ~eol:broker.b_frame_eol conn.conn_och
      "Invalid or missing destination: must be of the form /queue/xxx or /topic/xxx."

let ignore_command broker conn frame = return ()

let command_handlers = Hashtbl.create 13
let register_command (name, f) =
  Hashtbl.add command_handlers (String.uppercase name) f

let with_receipt f broker conn frame =
  f broker conn frame >>
  STOMP.handle_receipt ~eol:broker.b_frame_eol conn.conn_och frame

let () =
  List.iter register_command
    [
      "SUBSCRIBE", with_receipt cmd_subscribe;
      "UNSUBSCRIBE", with_receipt cmd_unsubscribe;
      "SEND", with_receipt cmd_send;
      "DISCONNECT", cmd_disconnect;
      "BEGIN", with_receipt ignore_command;
      "COMMIT", with_receipt ignore_command;
      "ABORT", with_receipt ignore_command;
    ]

let handle_frame broker conn frame =
  try
    Hashtbl.find command_handlers (String.uppercase frame.STOMP.fr_command)
      broker conn frame
  with Not_found ->
    send_error broker conn "Unknown command %S." frame.STOMP.fr_command

let handle_connection broker conn =
  let rec loop () =
    lwt frame = STOMP.read_stomp_frame ~eol:broker.b_frame_eol conn.conn_ich in
    handle_frame broker conn frame >>
    loop ()
  in loop ()

let establish_connection broker fd addr =
  let ich = Lwt_io.of_fd Lwt_io.input fd in
  let och = Lwt_io.of_fd Lwt_io.output fd in
  lwt frame = STOMP.read_stomp_frame ~eol:broker.b_frame_eol ich in
    match String.uppercase frame.STOMP.fr_command with
        "CONNECT" ->
            (* TODO: prefetch *)
          let conn =
            {
              conn_id = new_conn_id ();
              conn_ich = ich;
              conn_och = och;
              conn_default_prefetch = -1;
              conn_pending_acks = 0;
              conn_queues = Hashtbl.create 13;
              conn_topics = Hashtbl.create 13;
            }
          in begin
            try_lwt
              STOMP.write_stomp_frame ~eol:broker.b_frame_eol och
                {
                  STOMP.fr_command = "CONNECTED";
                  fr_headers = ["session", string_of_int conn.conn_id];
                  fr_body = "";
                } >>
              handle_connection broker conn
            with End_of_file | Sys_error _ | Unix.Unix_error _ ->
                let pending_acks =
                  ACKS.elements
                    (Hashtbl.fold
                       (fun _ subs s -> ACKS.union subs.qs_pending_acks s)
                       conn.conn_queues
                       ACKS.empty) in
                eprintf "CONNECTION %d DONE with %d pending ACKS\n%!"
                  conn.conn_id
                  (List.length pending_acks);
                (* remove from connection set and subscription lists *)
                broker.b_connections <- CONNS.remove conn broker.b_connections;
                Hashtbl.iter
                  (fun k _ ->
                     try
                       let s = Hashtbl.find broker.b_topics k in
                         s := CONNS.remove conn !s
                     with Not_found -> ())
                  conn.conn_topics;
                Hashtbl.iter
                  (fun k _ ->
                     let rm s = SUBS.remove (conn, dummy_subscription) s in
                     try
                       let ls = Hashtbl.find broker.b_queues k in
                         ls.l_ready <- rm ls.l_ready;
                         ls.l_blocked <- rm ls.l_blocked;
                     with Not_found -> ())
                  conn.conn_queues;
                (* cancel all the waiters: they will re-queue the
                 * corresponding messages *)
                List.iter
                  (fun (id, sleep, w) ->
                     (* eprintf "have to re-queue %S\n%!" id; *)
                     wakeup w ())
                     (* wakeup_exn wakeup Lwt.Canceled) *)
                  pending_acks;
                return ()
             | e ->
                 eprintf "GOT EXCEPTION: %s\n%!" (Printexc.to_string e);
                 eprintf "backtrace:\n%s" (Printexc.get_backtrace ());
                 eprintf "backtrace status: %s\n%!" (string_of_bool (Printexc.backtrace_status ()));
                 Printexc.print_backtrace stderr;
                 Lwt_io.abort och
          end
      | _ -> Lwt_io.write och "ERROR\n\nExcepted CONNECT frame.\000\n" >>
             Lwt_io.flush och >>
             Lwt_io.abort ich

let make_broker ?(frame_eol = true) msg_store address =
  let sock = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Lwt_unix.setsockopt sock Unix.SO_REUSEADDR true;
  Lwt_unix.bind sock address;
  Lwt_unix.listen sock 1024;
  return {
    b_connections = CONNS.empty;
    b_queues = Hashtbl.create 13;
    b_topics = Hashtbl.create 13;
    b_socket = sock;
    b_frame_eol = frame_eol;
    b_msg_store = msg_store;
  }

let server_loop broker =
  let rec loop () =
    lwt (fd, addr) = Lwt_unix.accept broker.b_socket in
      ignore_result (establish_connection broker fd addr);
      loop ()
  in
    P.crash_recovery broker.b_msg_store >> loop ()
end (* Make functor *)
