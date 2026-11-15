# Column names that are not legal in Deephaven are changed when a file is read: "a b"
# becomes a_b and "2x" becomes column_2x.
# oracle: results legal
import base64
import os
import shutil
import tempfile

# begin files: names.parquet
FILES = {
    "names.parquet": (
        "UEFSMRUEFQgVCEwVAhUAEgAAAQAAABUAFRIVEiwVAhUQFQYVBhwAAAACAAAAAgEBAgAVBBUIFQhMFQIVABIAAAIA"
        "AAAVABUSFRIsFQIVEBUGFQYcAAAAAgAAAAIBAQIAFQQVCBUITBUCFQASAAADAAAAFQAVEhUSLBUCFRAVBhUGHAAA"
        "AAIAAAACAQECABUEGUw1ABgGc2NoZW1hFQYAFQIlAhgDYSBiABUCJQIYAjJ4ABUCJQIYAm9rABYCGRwZPCYAHBUC"
        "GTUABhAZGANhIGIVABYCFlwWXCYsJggpLBUEFQAVAgAVABUQFQIAPCkGGSYAAgAAACYAHBUCGTUABhAZGAIyeBUA"
        "FgIWXBZcJogBJmQpLBUEFQAVAgAVABUQFQIAPCkGGSYAAgAAACYAHBUCGTUABhAZGAJvaxUAFgIWXBZcJuQBJsAB"
        "KSwVBBUAFQIAFQAVEBUCADwpBhkmAAIAAAAWlAIWAiYIFpQCABkcGAxBUlJPVzpzY2hlbWEYrAIvLy8vLzlnQUFB"
        "QVFBQUFBQUFBS0FBd0FCZ0FGQUFnQUNnQUFBQUFCQkFBTUFBQUFDQUFJQUFBQUJBQUlBQUFBQkFBQUFBTUFBQUIw"
        "QUFBQU5BQUFBQVFBQUFDby8vLy9BQUFCQWhBQUFBQVVBQUFBQkFBQUFBQUFBQUFDQUFBQWIyc0FBSmovLy84QUFB"
        "QUJJQUFBQU5ULy8vOEFBQUVDRUFBQUFCUUFBQUFFQUFBQUFBQUFBQUlBQUFBeWVBQUF4UC8vL3dBQUFBRWdBQUFB"
        "RUFBVUFBZ0FCZ0FIQUF3QUFBQVFBQkFBQUFBQUFBRUNFQUFBQUJ3QUFBQUVBQUFBQUFBQUFBTUFBQUJoSUdJQUNB"
        "QU1BQWdBQndBSUFBQUFBQUFBQVNBQUFBQT0AGCBwYXJxdWV0LWNwcC1hcnJvdyB2ZXJzaW9uIDI2LjAuMRk8HAAA"
        "HAAAHAAAAE8CAABQQVIx"
    ),
}
# end files
root = os.path.join(tempfile.gettempdir(), "oxhaven_parquet_read_names")
shutil.rmtree(root, ignore_errors=True)
for name, data in FILES.items():
    os.makedirs(os.path.dirname(os.path.join(root, name)), exist_ok=True)
    with open(os.path.join(root, name), "wb") as f:
        f.write(base64.b64decode(data))

from deephaven.parquet import read

legal = read(root + "/names.parquet")
