"""Records what the Java reference reads from the CSV texts below.

    oracle/.venv/bin/python crates/oxhaven-csv/tests/data/record.py 10000

The argument is the port of a running reference server (`oracle/bin/start-java.sh`). The
result is `recorded.json` next to this file: for each case its text, its options and the
table Java made of it (column names, types and values) or the fact that the read failed.
`src/tests.rs` reads every case with this crate and compares.

A case is `(name, text, options)`. The options are the keyword arguments of
`deephaven.csv.read`, as Python source; a type in `header` is written `dht.<name>`.
"""
import json
import os
import random
import sys

from pydeephaven import Session

CASES = [
    # --- which type a column gets
    ("types", "A,B,C,D,E,F\n1,2.5,true,hello,2024-01-02T03:04:05Z,x\n2,3,false,world,2024-01-02T03:04:05.123456789Z,y\n", ""),
    ("types_with_nulls", "A,B,C,D,E,F\n1,2.5,true,hello,2024-01-02T03:04:05Z,x\n,,,,,\n", ""),
    ("int_widths", "A,B,C,D,E\n1,300,70000,3000000000,9223372036854775807\n-1,-300,-70000,-3000000000,-9223372036854775808\n", ""),
    ("null_values_widen", "A,B,C\n-2147483648,-9223372036854775808,9223372036854775808\n1,1,1\n", ""),
    ("long_limits", "A,B,C\n9223372036854775807,9223372036854775806,-9223372036854775807\n", ""),
    ("long_wraps", "A,B,C\n25000000000000000000,20000000000000000000,1\n", ""),
    ("int_then_wider", "A,B,C\n1,1,1\n3000000000,1.5,3000000000\n2,2,1.5\n,,\n7,7,7\n", ""),
    ("int_overflow_to_double", "A\n1\n99999999999999999999\n", ""),
    ("all_empty_column", "A,B\n1,\n2,\n", ""),
    ("no_rows", "A,B\n", ""),
    ("nulls_first", "A,B,C,D\n,,,\n,,,\n5,x,true,2024-01-02T03:04:05Z\n", ""),
    ("char", "A,B,C\nx,1,xy\ny,2,z\n", ""),
    ("char_of_digits", "A,B\n1,a\nx,5\n", ""),
    ("digits_then_char", "A,B\n1,12\n2,x\nx,y\n", ""),
    ("single_digit", "A\n5\n", ""),
    ("unicode_char", "A,B,C\né,\U0001f600,日\nx,y,z\n", ""),
    ("char_null_value", "A\n￿\nx\n", ""),
    ("nul_char", "A,B\n\x00,1\n", ""),
    ("booleans", "A,B,C,D\ntrue,TRUE,True,t\nfalse,FALSE,False,f\n", ""),
    ("boolean_mixed", "A,B,C,D\ntrue,1,true,x\n1,true,x,true\n", ""),
    ("doubles", "A,B,C,D,E,F,G\n1.0,1e5,NaN,Infinity,-inf,.5,1.\n2,1E-5,nan,-Infinity,inf,5.,+1\n", ""),
    ("double_nan", "A,B,C,D\nNaN,1,+NaN,-Infinity\n1,NaN,1,1\n", ""),
    ("double_java_forms", "A,B,C,D,E,F\n1d,1f,0x1p3,1e400,4.9e-324,1e-400\n2,2,2,2,2,2\n", ""),
    ("double_forms", "A,B,C,D,E,F,G,H\n1e,1e+,.,+.5,0x1.8p1,0x.p1,1.0f,-NaN\n", ""),
    ("double_forms_2", "A,B,C,D,E,F,G,H\n1e5d,Infinityd,infinity,1D,0X1P-2,0x1p,1e+05,00.5\n", ""),
    ("double_forms_3", "A,B,C,D,E,F\n\" 1.5 \",\"1.5\n\",0x10,0x1.fffffffffffffp1023,0x1p1024,1F\n", ""),
    ("double_forms_4", "A,B,C,D,E,F,G,H\n1.e5,0.,-.5e-3,1e99999999999,1e-99999999999,0e5,- 1,1 e5\n", ""),
    ("double_hex", "A,B,C,D,E,F,G\n0x1p-1074,0x1p-1075,0x1.8p-1075,0x.8p-1073,0x1.00000000000008p0,0x1.00000000000018p0,0x1.0000000000000800000000000000000000001p0\n", ""),
    ("double_hex_2", "A,B,C,D,E,F,G\n0xffffffffffffffffffffffffffffffffffffp0,0x1.fffffffffffff8p1023,0x0p0,0x0.0p99999,-0x1p0,0x1p+3d,0x1.p1\n", ""),
    # Java drops the hexadecimal digits after the first sixteen, without rounding
    ("double_hex_long", "A,B,C,D,E,F,G\n0x123456789abcdef01p0,0x123456789abcdef81p0,0xde0b6b3a763ffff8p0,0xde0b6b3a764000008p0,0x0.000000000000000000001p0,0x1.00000000000008p-1023,0x00000000000000001.8p0\n", ""),
    # an exponent stops growing at 1024 in Java's parser
    ("double_hex_exponent", "A,B,C,D\n0x0." + "0" * 300 + "1p10240,0x1" + "0" * 300 + "p-10240,0x1p10240,0x1p-10750\n", ""),
    ("double_exponent", "A,B,C\n1e10240,1e-10240,0." + "0" * 800 + "1e1100\n", ""),
    ("double_exponent_short", "A,B\n0." + "0" * 800 + "1e10240,1" + "0" * 800 + "e-10240\n", ""),
    ("double_null_value", "A,B\n-1.7976931348623157e308,1.7976931348623157e308\n1,1\n", ""),
    ("double_precision", "A,B,C\n0.1,123456789012345678901234567890,2.2250738585072011e-308\n0.30000000000000004,1.7976931348623157E308,8.41e21\n", ""),
    ("negative_zero", "A,B\n-0,-0.0\n1,1\n", ""),
    ("signs_and_zeros", "A,B,C,D,E,F\n007,+5,1_000,0x10,-,+\n8,6,2,3,1,1\n", ""),
    # --- date-times
    ("datetimes", "A,B,C,D,E\n2024-01-02T03:04:05Z,2024-01-02T03:04:05+01:00,2024-01-02 03:04:05Z,2024-01-02T03:04:05 NY,2024-01-02\n2024-01-02T03:04Z,2024-01-02T03:04:05.5-05:00,2024-01-02 03:04:05.123Z,2024-01-02T03:04:05 UTC,2024-01-03\n", ""),
    ("datetime_without_zone", "A,B\n2024-01-02T03:04:05,2024-01-02T03:04:05.123456789\n2024-01-02T03:04:06,2024-01-02T03:04:05\n", ""),
    ("datetime_compact", "A,B,C,D,E\n20240102T030405Z,20240102T03Z,2024-01-02T03Z,\"2024-01-02T03:04:05,5Z\",20240102T0304Z\n", ""),
    ("datetime_offsets", "A,B,C,D,E,F\n2024-01-02T03:04:05+01,2024-01-02T03:04:05-0130,2024-01-02T03:04:05+01:30,2024-01-02T03:04:05.123456789123Z,2024-01-02T03:04:05+1:30,2024-01-02T03:04:05+99:99\n", ""),
    ("datetime_short_fields", "A,B,C,D,E\n1-02-03T04:05:06Z,2024-2-03T04:05:06Z,2024-02-3T04:05:06Z,2024-02-03T4:05:06Z,2024-02-03T04:5:6Z\n", ""),
    ("datetime_far", "A,B,C,D\n9999-12-31T23:59:59Z,0000-01-01T00:00:00Z,2262-04-11T23:47:16.854775807Z,2262-04-11T23:47:16.854775808Z\n", ""),
    ("datetime_null_value", "A\n1677-09-21T00:12:43.145224192Z\n", ""),
    ("datetime_then_other", "A,B,C\n2024-01-02T03:04:05Z,1,2024-01-02T03:04:05Z\nx,2024-01-02T03:04:05Z,1\n", ""),
    ("datetime_fixed_zones", "A,B,C,D,E,F\n2024-01-02T03:04:05 UTC,2024-01-02T03:04:05 utc,2024-01-02T03:04:05 Etc/UTC,2024-01-02T03:04:05 GMT,2024-01-02T03:04:05 Etc/GMT+5,2024-01-02T03:04:05 SystemV/EST5\n", ""),
    ("datetime_key_of_a_zone", "A,B\n2024-01-02T03:04:05 V:C,2024-01-02T03:04:05 Z\n", ""),
    ("datetime_regions", "A,B,C\n2024-01-02T03:04:05 ET,2024-01-02T03:04:05 JP,2024-01-02T03:04:05 america/new_york\n", ""),
    ("datetime_region_then_text", "A\n2024-01-02T03:04:05 ET\nx\n", ""),
    ("datetime_not_a_zone", "A,B,C\n2024-01-02T03:04:05 NY,2024-01-02T03:04:05 ,2024-01-02T03:04:05 AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n", ""),
    ("datetime_bad_month", "A\n2024-13-02T03:04:05Z\n", ""),
    ("datetime_bad_day", "A\n2024-02-30T03:04:05Z\n", ""),
    ("datetime_not_leap", "A\n2023-02-29T03:04:05Z\n", ""),
    ("datetime_leap", "A\n2024-02-29T03:04:05Z\n", ""),
    ("datetime_hour_24", "A\n2024-02-03T24:00:00Z\n", ""),
    ("datetime_second_60", "A\n2024-02-03T23:59:60Z\n", ""),
    ("datetime_bad_then_junk", "A\n2024-13-02T03:04:05Zjunk\n", ""),
    ("datetime_bad_in_region", "A\n2024-13-02T03:04:05 ET\n", ""),
    ("datetime_bad_after_text", "A\nx\n2024-13-02T03:04:05Z\n", ""),
    ("datetime_bad_later", "A\n2024-01-02T03:04:05Z\n2024-13-02T03:04:05Z\n", ""),
    # --- cells, quotes and line ends
    ("spaces", "A,B,C\n 1 , hello ,\" quoted \"\n 2 ,  x,\"y\"\n", ""),
    ("tabs", "A,B\n\t1\t,\tx\t\n2,y\n", ""),
    ("quoted_numbers", "A,B\n\"1\",\"true\"\n\"2\",\"false\"\n", ""),
    ("quoted_empty", "A,B\n\"\",1\nx,2\n", ""),
    ("quoted_spaces_in_numbers", "A,B\n\" 1\",\"true \"\n\"2 \",\" false\"\n", ""),
    ("quoted_newline", "A,B\n\"x\ny\",2\n\"a\r\nb\",3\n", ""),
    ("quoted_quote", "A,B\n\"x\"\"y\",\"\"\"\"\n", ""),
    ("junk_after_quote", "A,B\n\"x\"y,2\n", ""),
    ("quote_after_quote", "A,B\n\"x\" \",2\n", ""),
    ("space_after_quote", "A,B\n\"x\"  ,2\n", ""),
    ("unclosed_quote", "A,B\n\"x,2\n", ""),
    ("quote_inside_cell", "A,B\nx\"y\"z,2\n", ""),
    ("quoted_last_cell_at_end", "A,B\n1,\"x\"", ""),
    ("crlf", "A,B\r\n1,2\r\n3,4\r\n", ""),
    ("cr_only", "A,B\r1,2\r3,4\r", ""),
    ("no_final_newline", "A,B\n1,2", ""),
    ("header_only", "A,B", ""),
    ("empty_file", "", ""),
    ("trailing_delimiter", "A,B,\n1,2,\n3,4,\n", ""),
    ("trailing_delimiter_with_data", "A,B,\n1,2,x\n", ""),
    ("empty_header_line", "\n1\n2\n", ""),
    ("empty_header_line_empty_rows", "\n\n\n", ""),
    ("empty_header_line_only", "\n", ""),
    ("quoted_empty_header_only", "\"\"", ""),
    ("bom", "﻿A,B\n1,2\n", ""),
    # --- options
    ("empty_line_one_column", "A\n1\n\n3\n", ""),
    ("empty_line_two_columns", "A,B\n1,2\n\n3,4\n", ""),
    ("empty_line_ignored", "A,B\n1,2\n\n3,4\n", "ignore_empty_lines=True"),
    ("empty_line_ignored_one_column", "A\n1\n\n3\n", "ignore_empty_lines=True"),
    ("blank_line_ignored", "A,B\n1,2\n   \n3,4\n", "ignore_empty_lines=True"),
    ("missing_cells", "A,B,C\n1,2\n3,4,5\n", ""),
    ("missing_cells_allowed", "A,B,C\n1,2\n3,4,5\n", "allow_missing_columns=True"),
    ("excess_cells", "A,B\n1,2,3\n", ""),
    ("excess_cells_ignored", "A,B\n1,2,3\n4,5\n", "ignore_excess_columns=True"),
    ("surrounding_spaces_kept", "A,B,C\n 1 , x, \"q\"\n2,y,z\n", "ignore_surrounding_spaces=False"),
    ("trim", "A,B\n\" x \",\" 1 \"\n", "trim=True"),
    ("no_trim", "A,B\n\" x \",\" 1 \"\n", ""),
    ("delimiter", "A|B\n1|2,3\n", "delimiter='|'"),
    ("delimiter_tab", "A\tB\n1\t x \n", "delimiter='\\t'"),
    ("delimiter_space", "A B\n1  2\n", "delimiter=' '"),
    ("delimiter_space_ok", "A B\n1 2\n", "delimiter=' '"),
    ("quote", "A,B\n'x,y',\"z\"\n", "quote=\"'\""),
    ("delimiter_not_ascii", "A,B\n1,2\n", "delimiter='\\u00e9'"),
    ("headless", "1,x\n2,y\n", "headless=True"),
    ("headless_empty", "", "headless=True"),
    ("header_row", "junk\nmore junk,x,y\nA,B\n1,2\n", "header_row=2"),
    ("header_row_too_far", "A,B\n1,2\n", "header_row=5"),
    ("header_row_headless", "A,B\n1,2\n", "header_row=1, headless=True"),
    ("skip_rows", "A\n1\n2\n3\n4\n", "skip_rows=2"),
    ("num_rows", "A\n1\n2\n3\n4\n", "num_rows=2"),
    ("skip_and_num_rows", "A\n1\n2\n3\n4\n", "skip_rows=1, num_rows=2"),
    ("skip_rows_beyond", "A\n1\n2\n", "skip_rows=10"),
    ("num_rows_zero", "A\n1\n2\n", "num_rows=0"),
    ("num_rows_negative", "A\n1\n2\n", "num_rows=-1"),
    ("skip_rows_negative", "A\n1\n2\n", "skip_rows=-1"),
    ("num_rows_counts_ignored_lines", "A\n1\n\n\n2\n3\n", "num_rows=3, ignore_empty_lines=True"),
    ("skip_rows_counts_ignored_lines", "A\n1\n\n\n2\n3\n", "skip_rows=3, ignore_empty_lines=True"),
    ("skipped_row_is_checked", "A,B\n1,2,3,4\n5,6\n", "skip_rows=1"),
    ("skip_rows_headless", "1,x\n2,y\n3,z\n", "headless=True, skip_rows=1"),
    # --- columns the caller names
    ("header", "A,B\n1,2\n3,4\n", "header={'X': dht.double, 'Y': dht.string}"),
    ("header_headless", "1,2\n3,4\n", "header={'X': dht.double, 'Y': dht.string}, headless=True"),
    ("header_fewer", "A,B\n1,2\n", "header={'X': dht.int32}"),
    ("header_more", "A,B\n1,2\n", "header={'X': dht.int32, 'Y': dht.int32, 'Z': dht.int32}"),
    ("header_more_allowed", "A,B\n1,2\n", "header={'X': dht.int32, 'Y': dht.int32, 'Z': dht.int32}, allow_missing_columns=True"),
    ("header_all_types", "a,b,c,d,e,f,g,h,i,j\ntrue,1,x,2,3,4,1.5,2.5,s,2024-01-02T03:04:05Z\n,,,,,,,,,\n",
     "header={'Bo': dht.bool_, 'By': dht.byte, 'Ch': dht.char, 'Sh': dht.short, 'In': dht.int32, 'Lo': dht.long, 'Fl': dht.float32, 'Do': dht.double, 'St': dht.string, 'Ts': dht.Instant}"),
    ("header_no_rows", "a,b,c,d,e,f,g,h,i,j\n",
     "header={'Bo': dht.bool_, 'By': dht.byte, 'Ch': dht.char, 'Sh': dht.short, 'In': dht.int32, 'Lo': dht.long, 'Fl': dht.float32, 'Do': dht.double, 'St': dht.string, 'Ts': dht.Instant}"),
    ("header_int_fails", "A\n1\nx\n", "header={'X': dht.int32}"),
    ("header_byte_range", "A\n127\n128\n", "header={'X': dht.byte}"),
    ("header_byte_ok", "A\n127\n-127\n", "header={'X': dht.byte}"),
    ("header_byte_null_value", "A\n-128\n", "header={'X': dht.byte}"),
    ("header_short_range", "A\n-32767\n40000\n", "header={'X': dht.short}"),
    ("header_short_ok", "A\n-32767\n32767\n", "header={'X': dht.short}"),
    ("header_int_null_value", "A\n-2147483648\n", "header={'X': dht.int32}"),
    ("header_long_null_value", "A\n-9223372036854775808\n", "header={'X': dht.long}"),
    ("header_float", "A,B,C\n0.1,3.4028234e38,16777217\n1e-50,-1e-46,NaN\n", "header={'X': dht.float32, 'Y': dht.float32, 'Z': dht.float32}"),
    ("header_float_range", "A\n3.5e38\n", "header={'X': dht.float32}"),
    ("header_float_just_over", "A\n3.4028235e38\n", "header={'X': dht.float32}"),
    ("header_float_infinite", "A\nInfinity\n-Infinity\n", "header={'X': dht.float32}"),
    ("header_float_null_value", "A\n-3.4028234663852886E38\n", "header={'X': dht.float32}"),
    ("header_char_long", "A\nxy\n", "header={'X': dht.char}"),
    ("header_boolean_of_number", "A\n1\n", "header={'X': dht.bool_}"),
    ("header_string", "A\n1\n\ntrue\n", "header={'X': dht.string}"),
    ("header_instant_fails", "A\n2024-01-02\n", "header={'X': dht.Instant}"),
    ("header_empty", "A\n1\n", "header={}"),
    ("header_names_made_legal", "A,B\n1,2\n", "header={'my col': dht.int32, 'in': dht.int32}"),
    # --- column names
    ("names", "First Name,last-name,2nd,int,i,a.b,A B,A_B,ünï,$x,_,true,null,var,record,strictfp,goto,k,in,not,ii,enum,assert,x y-z,1,_1,a$\n" + ",".join(["1"] * 27) + "\n", ""),
    ("names_quoted", "\"A B\",\" C \",\"D\"\"E\"\n1,2,3\n", ""),
    ("names_repeated", "A,A,B,B2,B\n1,2,3,4,5\n", ""),
    ("names_repeated_2", "A2,A,A\n1,2,3\n", ""),
    ("names_repeated_3", "A,A,A2\n1,2,3\n", ""),
    ("names_digits", "1,1,column_1\n1,2,3\n", ""),
    ("names_empty", "A,,B\n1,2,3\n", ""),
    ("names_nothing_left", "A,!!\n1,2\n", ""),
    ("names_not_ascii", "ü,B\n1,2\n", ""),
    ("names_reserved_once_stripped", "i!,x\n1,2\n", ""),
]

# Columns of a few cells drawn from these, to walk the inference through the orders in
# which types can follow each other.
POOL = [
    "", "", "", "1", "7", "-3", "12", "300", "2147483647", "-2147483648", "3000000000",
    "9223372036854775807", "-9223372036854775808", "99999999999999999999", "1.5", "-0.0",
    "1e5", "NaN", "Infinity", ".5", "5.", "1d", "0x1p3", "+1", " 1", "true", "false", "TRUE",
    "False", "t", "x", "y", "é", "\U0001f600", "xy", "hello", "-", "+", ".", "1e",
    "2024-01-02T03:04:05Z", "2024-01-02T03:04:05.5+01:00", "2024-01-02 03:04Z",
    "2024-01-02T03:04:05 UTC", "2024-01-02", "20240102T030405Z", "2024-01-02T03:04:05",
]


def generated():
    rng = random.Random(7)
    cases = []
    for n in range(400):
        # a few cell texts per column, so that runs of one family are common
        some = rng.sample(POOL, rng.randint(1, 4))
        cells = [rng.choice(some) for _ in range(rng.randint(1, 7))]
        text = "A\n" + "".join(f'"{cell}"\n' for cell in cells)
        cases.append((f"generated_{n:03d}", text, ""))
    return cases


SERVER = r'''
import json as _json, os as _os, struct as _struct, tempfile as _tempfile
from deephaven import csv as _csv, dtypes as dht, new_table as _new_table
from deephaven.column import string_col as _string_col

_NULLS = {"byte": -128, "short": -32768, "int": -2147483648, "long": -(2**63),
          "float": -3.4028234663852886e38, "double": -1.7976931348623157e308}

def _record(cases):
    out = []
    directory = _tempfile.mkdtemp()
    for n, (name, text, options) in enumerate(cases):
        path = _os.path.join(directory, f"case{n}.csv")
        with open(path, "wb") as f:
            f.write(text.encode("utf-8"))
        result = {"name": name, "text": text, "options": options}
        try:
            table = _csv.read(path, **eval("dict(" + options + ")"))
        except Exception as e:
            result["error"] = type(e).__name__
            out.append(_json.dumps(result))
            continue
        types = [c.data_type.j_name.split(".")[-1] for c in table.columns]
        names = [c.name for c in table.columns]
        # under other names: `iter_tuple` does not take every column name
        views = []
        for n, (column, kind) in enumerate(zip(names, types)):
            if kind == "char":
                views.append(f"c{n} = (int){column}")
            elif kind == "Instant":
                # not epochNanos, which refuses the last second a long of nanoseconds holds
                views.append(
                    f"c{n} = isNull({column}) ? NULL_LONG"
                    f" : {column}.getEpochSecond() * 1000000000L + {column}.getNano()"
                )
            else:
                views.append(f"c{n} = {column}")
        columns = [{"name": c, "type": t, "values": []} for c, t in zip(names, types)]
        rows = 0
        for row in (table.view(views) if views else table).iter_tuple():
            rows += 1
            for column, value in zip(columns, row):
                kind = column["type"]
                null = {"char": _NULLS["int"], "Instant": _NULLS["long"]}.get(kind, _NULLS.get(kind))
                if value is None or (null is not None and value == null):
                    column["values"].append(None)
                elif kind in ("float", "double"):
                    # the bits of the double, a float widened
                    bits = _struct.unpack("<q", _struct.pack("<d", float(value)))[0]
                    column["values"].append(str(bits))
                elif kind == "Boolean":
                    column["values"].append("true" if value else "false")
                else:
                    column["values"].append(str(value))
        result["columns"] = columns
        result["rows"] = table.size
        out.append(_json.dumps(result))
    return out

_csv_recorded = _new_table([_string_col("R", _record(_CASES))])
'''


def main():
    port = int(sys.argv[1])
    cases = CASES + generated()
    assert len({name for name, _, _ in cases}) == len(cases), "a case name is used twice"
    session = Session(
        host="127.0.0.1", port=port, auth_token="oxhaven",
        auth_type="io.deephaven.authentication.psk.PskAuthenticationHandler",
    )
    # ascii(): the reference's console does not take every character in a script
    session.run_script("_CASES = " + ascii(cases) + "\n" + SERVER)
    recorded = session.open_table("_csv_recorded").to_arrow().column("R").to_pylist()
    session.run_script("del _csv_recorded, _CASES")
    session.close()
    # the options once more as data, a type by its name in `deephaven.dtypes`; `header`
    # as a list of pairs, because the order of its names is what assigns them to columns
    type_names = type("TypeNames", (), {"__getattr__": lambda self, name: name})()
    lines = []
    for (_, _, options), line in zip(cases, recorded):
        record = json.loads(line)
        args = eval("dict(" + options + ")", {"dht": type_names})
        if "header" in args:
            args["header"] = list(args["header"].items())
        record["args"] = args
        lines.append(json.dumps(record))
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "recorded.json")
    with open(out, "w") as f:
        f.write("[\n" + ",\n".join(lines) + "\n]\n")
    failed = sum("error" in json.loads(line) for line in lines)
    print(f"{len(lines)} cases recorded, {failed} of them reads that fail")


if __name__ == "__main__":
    main()
