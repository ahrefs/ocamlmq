(* Copyright (c) 2010 Mauricio Fernández <mfp@acm.org> *)

type destination = Queue of string | Topic of string | Control of string

type message = {
  msg_id : string;
  msg_destination : destination;
  msg_priority : int;
  msg_timestamp : float;
  msg_body : string;
  msg_ack_timeout : float;
  msg_headers : (string * string) list;
}

let string_of_destination = function
    Topic n -> "/topic/" ^ n
  | Queue n -> "/queue/" ^ n
  | Control n -> "/control/" ^ n

let destination_name = function
    Topic n | Queue n -> n
  | Control _n -> invalid_arg "Mq_types.destination_name"

let serialize_headers hs =
  let b = Buffer.create 20 in
  List.iter
    (fun (k, v) ->
       Buffer.add_string b k; 
       Buffer.add_char b ':';
       Buffer.add_string b v;
       Buffer.add_char b '\n';
    )
    hs;
  Buffer.contents b

let deserialize_headers str =
  let kvstrings = BatString.nsplit str ~by:"\n" in
  let rec loop rev_acc strs =
    match strs with
    | [] | "" :: _ -> List.rev rev_acc
    | h :: t ->
        if h = ""
        then
          loop rev_acc t
        else
          match (try Some (BatString.split h ~by:":") with _ -> None) with
          | None -> failwith "deserialize_headers: no ':'"
          | Some kv -> loop (kv :: rev_acc) t
  in
    loop [] kvstrings
