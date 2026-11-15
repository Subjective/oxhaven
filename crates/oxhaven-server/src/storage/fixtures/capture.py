#!/usr/bin/env python3
"""Records what the Java reference's StorageService answers over a real directory.

No Java counterpart: test tooling. The output is `java.txt` next to this script, which the
tests of the storage service replay against this server (`../tests.rs`). Run it with the
oracle's Python environment against a Java server whose storage is a directory named
`storage` that this script can write to:

    T=$(mktemp -d)
    oracle/ide/bin/start-java-ide.sh 10958 scope --jvm-arg=-Dstorage.path=$T/storage
    oracle/.venv/bin/python crates/oxhaven-server/src/storage/fixtures/capture.py \
        --server 127.0.0.1:10958 --psk oxhaven --root $T/storage \
        > crates/oxhaven-server/src/storage/fixtures/java.txt
    oracle/bin/stop-java.sh 10958

The storage has to be as the server made it (the empty directories `layouts` and
`notebooks`). The script makes a directory `outside` next to it, which holds what a client
must not reach.

Format of the output, one case after the other, to be replayed in order:

    == <case name>
    fs: <step> <arguments>                    done in the file system before the call
    call: StorageService/<Method>
    auth: none                                only for a call sent without a session
    request: <hex of the request message>
    # <the request, protobuf text format>
    status: <gRPC code name>[: <message>]
    message: <hex of the response message>
    # <the same message, protobuf text format>

`fs:` steps, with paths relative to the directory that holds `storage` and `outside`:

    fs: write <path> <hex of the contents>
    fs: mkdir <path>
    fs: symlink <path> <target as written into the link>

Error ids (`Details Logged w/ID '...'`) differ from run to run, and `ListItems` lists a
directory in the order the file system has it; nothing else should differ.
"""

import argparse
import os

import grpc
from google.protobuf import text_format

from deephaven_core.proto import config_pb2, config_pb2_grpc, storage_pb2, storage_pb2_grpc

CALL_TIMEOUT = 10
PSK_AUTH_TYPE = "io.deephaven.authentication.psk.PskAuthenticationHandler"


class Recorder:
    def __init__(self, address: str, psk: str, root: str):
        self.base = os.path.dirname(os.path.abspath(root))
        channel = grpc.insecure_channel(address)
        call = config_pb2_grpc.ConfigServiceStub(channel).GetConfigurationConstants.with_call
        _, info = call(
            config_pb2.ConfigurationConstantsRequest(),
            metadata=[("authorization", f"{PSK_AUTH_TYPE} {psk}")],
        )
        self.metadata = [("authorization", dict(info.initial_metadata())["authorization"])]
        self.stub = storage_pb2_grpc.StorageServiceStub(channel)
        self.pending_fs = []

    def fs(self, step: str, path: str, argument: str = ""):
        """Does a file system step now and records it with the next case."""
        full = os.path.join(self.base, path)
        if step == "write":
            with open(full, "wb") as file:
                file.write(argument.encode())
            argument = argument.encode().hex()
        elif step == "mkdir":
            os.mkdir(full)
        elif step == "symlink":
            os.symlink(argument, full)
        else:
            raise ValueError(step)
        self.pending_fs.append(f"fs: {step} {path} {argument}".rstrip())

    def call(self, name: str, method: str, authenticated: bool = True, **fields):
        request = getattr(storage_pb2, f"{method}Request")(**fields)
        print(f"== {name}")
        for line in self.pending_fs:
            print(line)
        self.pending_fs = []
        print(f"call: StorageService/{method}")
        if not authenticated:
            print("auth: none")
        print(f"request: {request.SerializeToString().hex()}")
        print(f"# {text_format.MessageToString(request, as_one_line=True)}")
        try:
            response = getattr(self.stub, method)(
                request, metadata=self.metadata if authenticated else None, timeout=CALL_TIMEOUT
            )
        except grpc.RpcError as error:
            details = (error.details() or "").replace("\\", "\\\\").replace("\n", "\\n")
            print(f"status: {error.code().name}: {details}")
            print()
            return None
        print("status: OK")
        print(f"message: {response.SerializeToString().hex()}")
        print(f"# {text_format.MessageToString(response, as_one_line=True)}")
        print()
        return response


def record(r: Recorder):
    items = lambda name, **f: r.call(name, "ListItems", **f)
    mkdir = lambda name, path: r.call(name, "CreateDirectory", path=path)
    save = lambda name, path, contents, overwrite=False: r.call(
        name, "SaveFile", path=path, contents=contents, allow_overwrite=overwrite
    )
    fetch = lambda name, path, **f: r.call(name, "FetchFile", path=path, **f)
    move = lambda name, old, new, overwrite=False: r.call(
        name, "MoveItem", old_path=old, new_path=new, allow_overwrite=overwrite
    )
    delete = lambda name, path: r.call(name, "DeleteItem", path=path)

    # --- what the File Explorer does -----------------------------------------------------
    items("list_root_at_start", path="/")
    mkdir("mkdir", "/notebooks/dir")
    mkdir("mkdir_again", "/notebooks/dir")
    mkdir("mkdir_no_parent", "/notebooks/nope/dir")
    mkdir("mkdir_no_leading_slash", "notebooks/plain")
    mkdir("mkdir_trailing_slash", "/notebooks/slash/")
    mkdir("mkdir_root", "/")
    mkdir("mkdir_root_empty_path", "")
    mkdir("mkdir_root_through_parent", "/notebooks/..")

    saved = save("save_new", "/notebooks/a.py", b"print('a')\n")
    save("save_existing_no_overwrite", "/notebooks/a.py", b"other")
    save("save_existing_overwrite", "/notebooks/a.py", b"print('a2')\n", overwrite=True)
    save("save_new_with_overwrite", "/notebooks/b.py", b"b = 1\n", overwrite=True)
    save("save_empty", "/notebooks/empty.py", b"")
    save("save_no_directory", "/notebooks/nope/a.py", b"x")
    save("save_onto_directory", "/notebooks/dir", b"x")
    save("save_onto_directory_overwrite", "/notebooks/dir", b"x", overwrite=True)
    save("save_below_a_file", "/notebooks/a.py/x", b"x")
    save("save_root", "/", b"x")
    save("save_root_overwrite", "", b"x", overwrite=True)
    save("save_unicode_name", "/notebooks/café ☃.py", "s = '☃'\n".encode())
    save("save_binary", "/notebooks/bytes.bin", bytes(range(256)))

    fetched = fetch("fetch", "/notebooks/a.py")
    fetch("fetch_same_etag", "/notebooks/a.py", etag=fetched.etag)
    fetch("fetch_other_etag", "/notebooks/a.py", etag=saved.etag)
    fetch("fetch_empty_etag", "/notebooks/a.py", etag="")
    fetch("fetch_empty_file", "/notebooks/empty.py")
    fetch("fetch_binary", "/notebooks/bytes.bin")
    fetch("fetch_missing", "/notebooks/nope.py")
    fetch("fetch_directory", "/notebooks/dir")
    fetch("fetch_root", "/")
    fetch("fetch_below_a_file", "/notebooks/a.py/x")
    fetch("fetch_no_leading_slash", "notebooks/a.py")

    items("list_notebooks", path="/notebooks")
    items("list_notebooks_glob", path="/notebooks", filter_glob="*.py")
    items("list_a_file", path="/notebooks/a.py")
    items("list_root_after", path="")

    move("move_rename", "/notebooks/b.py", "/notebooks/c.py")
    move("move_missing", "/notebooks/b.py", "/notebooks/d.py")
    move("move_onto_existing", "/notebooks/c.py", "/notebooks/a.py")
    move("move_onto_existing_overwrite", "/notebooks/c.py", "/notebooks/a.py", overwrite=True)
    fetch("fetch_after_move", "/notebooks/a.py")
    move("move_into_directory_path", "/notebooks/a.py", "/notebooks/dir/a.py")
    move("move_no_target_directory", "/notebooks/dir/a.py", "/notebooks/nope/a.py")
    move("move_to_itself", "/notebooks/dir/a.py", "/notebooks/dir/a.py")
    move("move_to_itself_overwrite", "/notebooks/dir/a.py", "/notebooks/dir/a.py", overwrite=True)
    move("move_file_onto_directory", "/notebooks/empty.py", "/notebooks/plain")
    move("move_file_onto_empty_directory_overwrite", "/notebooks/empty.py", "/notebooks/plain", overwrite=True)
    move("move_file_onto_full_directory_overwrite", "/notebooks/plain", "/notebooks/dir", overwrite=True)
    move("move_directory", "/notebooks/dir", "/notebooks/moved")
    move("move_directory_into_itself", "/notebooks/moved", "/notebooks/moved/inner")
    move("move_directory_onto_file", "/notebooks/moved", "/notebooks/bytes.bin")
    move("move_to_root", "/notebooks/bytes.bin", "/")
    move("move_to_root_overwrite", "/notebooks/bytes.bin", "", overwrite=True)
    move("move_root", "/", "/notebooks/root")
    items("list_after_moves", path="/notebooks")
    items("list_moved", path="/notebooks/moved")

    delete("delete_file", "/notebooks/bytes.bin")
    delete("delete_again", "/notebooks/bytes.bin")
    delete("delete_full_directory", "/notebooks/moved")
    delete("delete_in_directory", "/notebooks/moved/a.py")
    delete("delete_empty_directory", "/notebooks/moved")
    delete("delete_root", "/")
    delete("delete_root_empty_path", "")
    delete("delete_root_through_parent", "/layouts/..")
    delete("delete_below_a_file", "/notebooks/plain/x")
    items("list_after_deletes", path="/notebooks")

    # --- paths that try to leave the storage ----------------------------------------------
    r.fs("write", "outside/secret.txt", "secret\n")
    r.fs("mkdir", "outside/dir")
    for name, path in [
        ("parent", "/../outside/secret.txt"),
        ("parent_no_slash", "../outside/secret.txt"),
        ("parent_deeper", "/notebooks/../../outside/secret.txt"),
        ("parent_far", "/../../../../../../../../../../etc/hostname"),
        ("absolute", "//etc/hostname"),
        ("dot_dot_only", "/.."),
    ]:
        fetch(f"escape_fetch_{name}", path)
        save(f"escape_save_{name}", path, b"x", overwrite=True)
        delete(f"escape_delete_{name}", path)
        mkdir(f"escape_mkdir_{name}", path + ".d")
        items(f"escape_list_{name}", path=path)
        move(f"escape_move_from_{name}", path, "/notebooks/stolen", overwrite=True)
        move(f"escape_move_to_{name}", "/notebooks/plain", path, overwrite=True)
    # leaves the storage and comes back through its own name
    fetch("outside_and_back_fetch", "/../storage/notebooks/plain")
    items("outside_and_back_list", path="/../storage/notebooks")
    items("outside_prefix_of_name", path="/../storage2")
    fetch("nul_character", "/notebooks/a\0b")
    save("nul_character_save", "/notebooks/a\0b", b"x")
    fetch("backslash_is_a_name", "/notebooks\\plain")
    save("long_name", "/notebooks/" + "n" * 300, b"x")

    # --- symbolic links, which only someone with the directory can make --------------------
    r.fs("symlink", "storage/notebooks/link_file_out", "../../outside/secret.txt")
    r.fs("symlink", "storage/notebooks/link_dir_out", "../../outside/dir")
    r.fs("symlink", "storage/notebooks/link_in", "plain")
    r.fs("symlink", "storage/notebooks/link_dir_in", "../layouts")
    fetch("link_fetch_file_out", "/notebooks/link_file_out")
    fetch("link_fetch_in", "/notebooks/link_in")
    items("link_list_dir_out", path="/notebooks/link_dir_out")
    items("link_list_dir_in", path="/notebooks/link_dir_in")
    items("link_list_with_links", path="/notebooks")
    save("link_save_through_dir_out", "/notebooks/link_dir_out/planted.txt", b"planted")
    save("link_save_onto_file_out", "/notebooks/link_file_out", b"overwritten\n", overwrite=True)
    save("link_save_onto_file_out_no_overwrite", "/notebooks/link_file_out", b"x")
    save("link_save_through_dir_in", "/notebooks/link_dir_in/in.json", b"{}")
    mkdir("link_mkdir_through_dir_out", "/notebooks/link_dir_out/made")
    move("link_move_into_dir_out", "/notebooks/a.py", "/notebooks/link_dir_out/a.py")
    move("link_move_out_of_dir_out", "/notebooks/link_dir_out/planted.txt", "/notebooks/taken.txt")
    delete("link_delete_through_dir_out", "/notebooks/link_dir_out/made")
    delete("link_delete_file_link", "/notebooks/link_file_out")
    move("link_move_dir_link", "/notebooks/link_dir_out", "/notebooks/link_renamed")
    delete("link_delete_dir_link", "/notebooks/link_renamed")
    r.fs("symlink", "storage/notebooks/dangling", "nowhere")
    fetch("link_fetch_dangling", "/notebooks/dangling")
    items("link_list_with_dangling", path="/notebooks")
    save("link_save_onto_dangling", "/notebooks/dangling", b"x")
    save("link_save_onto_dangling_overwrite", "/notebooks/dangling", b"x", overwrite=True)
    delete("link_delete_dangling", "/notebooks/dangling")
    delete("link_delete_dangling_target", "/notebooks/nowhere")
    items("list_at_end", path="/notebooks")
    items("list_layouts_at_end", path="/layouts")

    # --- no session ---------------------------------------------------------------------
    r.call("unauthenticated_list", "ListItems", authenticated=False, path="/")
    r.call("unauthenticated_fetch", "FetchFile", authenticated=False, path="/notebooks/plain")
    r.call("unauthenticated_save", "SaveFile", authenticated=False, path="/notebooks/u", contents=b"x")
    r.call("unauthenticated_move", "MoveItem", authenticated=False, old_path="/notebooks/plain", new_path="/notebooks/u")
    r.call("unauthenticated_mkdir", "CreateDirectory", authenticated=False, path="/notebooks/u")
    r.call("unauthenticated_delete", "DeleteItem", authenticated=False, path="/notebooks/plain")
    # an invalid path without a session: which check comes first
    r.call("unauthenticated_escape", "FetchFile", authenticated=False, path="/../outside/secret.txt")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", required=True)
    parser.add_argument("--psk", default="oxhaven")
    parser.add_argument("--root", required=True, help="the server's storage.path; named storage")
    args = parser.parse_args()
    root = os.path.abspath(args.root)
    if os.path.basename(root) != "storage":
        parser.error("the storage directory has to be named storage")
    outside = os.path.join(os.path.dirname(root), "outside")
    # nothing is deleted here: the recording needs a storage nobody has used
    if os.path.exists(outside):
        parser.error(f"{outside} exists: start the server over a new directory")
    for name in sorted(os.listdir(root)):
        if name not in ("layouts", "notebooks") or os.listdir(os.path.join(root, name)):
            parser.error(f"{root} is not a new storage: start the server over a new directory")
    os.mkdir(outside)
    record(Recorder(args.server, args.psk, root))


if __name__ == "__main__":
    main()
