
type t

(** returns [(binlog, added_msgs, acked_msg_ids)] *)
val make : ?sync:bool -> string ->
  (t * Mq_types.message list * string list) Lwt.t
val truncate : t -> unit
val add : t -> Mq_types.message -> unit Lwt.t
(** [cancel binlog msg_id] *)
val cancel : t -> string -> unit Lwt.t

