# Reads that fail: the exception is DHError and its message says that reading failed. An
# unsigned value that does not fit a long fails the operation that reads it, not the read
# that makes the table.
# oracle: results good, bad, too_large
import base64
import os
import shutil
import tempfile

# begin files: flat/part0.parquet uint64_big.parquet no_map.parquet no_nested_list.parquet
FILES = {
    "flat/part0.parquet": (
        "UEFSMRUAFSQVJCwVBhUAFQYVBgAAAgAAAAMHAAAAAAAAAAAAAAAAFQAVJBUkLBUGFQAVBhUGAAACAAAAAwcAAAAA"
        "AQAAAAIAAAAVBBUwFTBMFQYVAAAABAAAAGYwXzAEAAAAZjBfMQQAAABmMF8yFQAVFBUULBUGFRAVBhUGAAACAAAA"
        "AwcCAyQAGRwWCBVGFgAAABkcFk4VRhYAAAAZHBbeARU2FgAAABUCGUxIBHJvb3QVBgAVAiUCGAFQJSJMrBMgEQAA"
        "ABUCJQIYAVglIkysEyARAAAAFQwlAhgBUyUATBwAAAAWBhkcGTwmABwVAhkVABkYAVAVABYGFkYWRiYIPBgEAAAA"
        "ABgEAAAAABYAKAQAAAAAGAQAAAAAABkcFQAVABUCAAAWlAIVFAAmABwVAhkVABkYAVgVABYGFkYWRiZOPBgEAgAA"
        "ABgEAAAAABYAKAQCAAAAGAQAAAAAABkcFQAVABUCAAAWqAIVFAAmABwVDBklABAZGAFTFQAWBhaAARaAASbeASaU"
        "ARw2ACgEZjBfMhgEZjBfMAAZLBUEFQAVAgAVABUQFQIAABa8AhUWABaMAhYGJggWjAIUAAAZHBgJZGVlcGhhdmVu"
        "GBJ7InZlcnNpb24iOiI0Mi45In0AGEpwYXJxdWV0LW1yIHZlcnNpb24gMS4xOC4yIChidWlsZCA3NGNkZjYxOWQ1"
        "YTE0Nzk1ZTAzNzA1Mzg5NjM3NmEzYTMzNzk1NjdjKRk8HAAAHAAAHAAAAJIBAABQQVIx"
    ),
    "uint64_big.parquet": (
        "UEFSMRUEFUAVQEwVCBUAEgAAAQAAAAAAAAAAAAAAAAAAgP///////////////////38VABUUFRQsFQoVEBUGFQYc"
        "AAAAAgAAAAMXAgPkABUEGSw1ABgGc2NoZW1hFQIAFQQlAhgBViUcTKwTQBIAAAAWChkcGRwmABwVBBk1AAYQGRgB"
        "VhUAFgoWlgEWlgEmZCYIKSwVBBUAFQIAFQAVEBUCADwpBhkmAggAAAAWlgEWCiYIFpYBABkcGAxBUlJPVzpzY2hl"
        "bWEYoAEvLy8vLzNBQUFBQVFBQUFBQUFBS0FBd0FCZ0FGQUFnQUNnQUFBQUFCQkFBTUFBQUFDQUFJQUFBQUJBQUlB"
        "QUFBQkFBQUFBRUFBQUFVQUFBQUVBQVVBQWdBQmdBSEFBd0FBQUFRQUJBQUFBQUFBQUVDRUFBQUFCZ0FBQUFFQUFB"
        "QUFBQUFBQUVBQUFCV0FBWUFDQUFFQUFZQUFBQkFBQUFBABggcGFycXVldC1jcHAtYXJyb3cgdmVyc2lvbiAyNi4w"
        "LjEZHBwAAABFAQAAUEFSMQ=="
    ),
    "no_map.parquet": (
        "UEFSMRUEFQoVCkwVAhUAEgAAAQAAAGsVABUgFSAsFQQVEBUGFQYcAAAAAgAAAAQAAwAAAAMCAAECABUEFQgVCEwV"
        "AhUAEgAAAQAAABUAFSAVICwVBBUQFQYVBhwAAAACAAAABAADAAAAAwMAAQIAFQQZXDUAGAZzY2hlbWEVAgA1AhgB"
        "VhUCFQJMLAAAADUEGAlrZXlfdmFsdWUVBAAVDCUAGANrZXklAEwcAAAAFQIlAhgFdmFsdWUAFgQZHBksJgAcFQwZ"
        "NQAGEBk4AVYJa2V5X3ZhbHVlA2tleRUAFgQWbBZsJi4mCCksFQQVABUCABUAFRAVAgA8FgIZJgQAGTYCAAIAAAAm"
        "ABwVAhk1AAYQGTgBVglrZXlfdmFsdWUFdmFsdWUVABYEFmoWaiaYASZ0KSwVBBUAFQIAFQAVEBUCADwpJgQAGUYC"
        "AAACAAAAFtYBFgQmCBbWAQAZHBgMQVJST1c6c2NoZW1hGPgCLy8vLy94QUJBQUFRQUFBQUFBQUtBQXdBQmdBRkFB"
        "Z0FDZ0FBQUFBQkJBQU1BQUFBQ0FBSUFBQUFCQUFJQUFBQUJBQUFBQUVBQUFBRUFBQUFxUC8vL3dBQUFSRVVBQUFB"
        "R0FBQUFBUUFBQUFCQUFBQUVBQUFBQUVBQUFCV0FBQUFWUC8vLzRqLy8vOEFBQUFOR0FBQUFDQUFBQUFFQUFBQUFn"
        "QUFBSEFBQUFBa0FBQUFCd0FBQUdWdWRISnBaWE1BaFAvLy94QUFGQUFJQUFZQUJ3QU1BQUFBRUFBUUFBQUFBQUFC"
        "QWhBQUFBQWdBQUFBQkFBQUFBQUFBQUFGQUFBQWRtRnNkV1VBQUFBSUFBd0FDQUFIQUFnQUFBQUFBQUFCSUFBQUFC"
        "QUFGQUFJQUFBQUJ3QU1BQUFBRUFBUUFBQUFBQUFBQlJBQUFBQVlBQUFBQkFBQUFBQUFBQUFEQUFBQWEyVjVBQVFB"
        "QkFBRUFBQUFBQUFBQUE9PQAYIHBhcnF1ZXQtY3BwLWFycm93IHZlcnNpb24gMjYuMC4xGSwcAAAcAAAAowIAAFBB"
        "UjE="
    ),
    "no_nested_list.parquet": (
        "UEFSMRUEFQgVCEwVAhUAEgAAAQAAABUAFSIVIiwVBBUQFQYVBhwAAAACAAAABAAEAAAAAwUAAAECABUEGWw1ABgG"
        "c2NoZW1hFQIANQIYAVYVAhUGTDwAAAA1BBgEbGlzdBUCADUCGAdlbGVtZW50FQIVBkw8AAAANQQYBGxpc3QVAgAV"
        "AiUCGAdlbGVtZW50ABYEGRwZHCYAHBUCGTUABhAZWAFWBGxpc3QHZWxlbWVudARsaXN0B2VsZW1lbnQVABYEFmwW"
        "bCYsJggpLBUEFQAVAgAVABUQFQIAPCk2BAAAGWYCAAAAAAIAAAAWbBYEJggWbAAZHBgMQVJST1c6c2NoZW1hGKwC"
        "Ly8vLy85Z0FBQUFRQUFBQUFBQUtBQXdBQmdBRkFBZ0FDZ0FBQUFBQkJBQU1BQUFBQ0FBSUFBQUFCQUFJQUFBQUJB"
        "QUFBQUVBQUFBRUFBQUFxUC8vL3dBQUFRd1VBQUFBR0FBQUFBUUFBQUFCQUFBQUVBQUFBQUVBQUFCV0FBQUExUC8v"
        "LzlELy8vOEFBQUVNRkFBQUFDQUFBQUFFQUFBQUFRQUFBQ2dBQUFBRUFBQUFhWFJsYlFBQUFBQUVBQVFBQkFBQUFC"
        "QUFGQUFJQUFZQUJ3QU1BQUFBRUFBUUFBQUFBQUFCQWhBQUFBQWdBQUFBQkFBQUFBQUFBQUFFQUFBQWFYUmxiUUFB"
        "QUFBSUFBd0FDQUFIQUFnQUFBQUFBQUFCSUFBQUFBQUFBQUE9ABggcGFycXVldC1jcHAtYXJyb3cgdmVyc2lvbiAy"
        "Ni4wLjEZHBwAAAAjAgAAUEFSMQ=="
    ),
}
# end files
root = os.path.join(tempfile.gettempdir(), "oxhaven_parquet_read_errors")
shutil.rmtree(root, ignore_errors=True)
for name, data in FILES.items():
    os.makedirs(os.path.dirname(os.path.join(root, name)), exist_ok=True)
    with open(os.path.join(root, name), "wb") as f:
        f.write(base64.b64decode(data))

from deephaven.parquet import ColumnInstruction, ParquetFileLayout, UnsignedLongTarget, read

good = read(root + "/flat/part0.parquet")
# oracle: fails
bad = read(root + "/flat/nope.parquet")
# oracle: step
# oracle: fails
bad = read(root + "/nope")
# oracle: step
# oracle: fails
os.makedirs(root + "/empty")
bad = read(root + "/empty")
# oracle: step
# oracle: fails
bad = read(root + "/flat/part0.parquet", is_refreshing=True)
# oracle: step
# oracle: fails
bad = read(root + "/flat", file_layout=ParquetFileLayout.SINGLE_FILE)
# oracle: step
# oracle: fails
bad = read(root + "/flat/part0.parquet", file_layout=ParquetFileLayout.FLAT_PARTITIONED)
# oracle: step
# oracle: fails
bad = read(root + "/flat", file_layout=ParquetFileLayout.METADATA_PARTITIONED)
# oracle: step
# oracle: fails
bad = read(root + "/flat/part0.parquet", col_instructions=[ColumnInstruction(column_name="Q")])
# oracle: step
# oracle: fails
bad = read(root + "/flat/part0.parquet", col_instructions=[
    ColumnInstruction(column_name="A", parquet_column_name="X"),
    ColumnInstruction(column_name="B", parquet_column_name="X"),
])
# oracle: step
# oracle: fails
bad = read(root + "/flat/part0.parquet", col_instructions=[
    ColumnInstruction(column_name="P", parquet_column_name="X"),
])
# oracle: step
# oracle: fails
bad = read(root + "/no_map.parquet")
# oracle: step
# oracle: fails
bad = read(root + "/no_nested_list.parquet")
# oracle: step
# oracle: fails
bad = read("")
# oracle: step
# oracle: fails
too_large = read(root + "/uint64_big.parquet", col_instructions=[
    ColumnInstruction(column_name="V", parquet_column_name="V", unsigned_long_target=UnsignedLongTarget.LONG),
])
bad = too_large.select()
