#!/usr/bin/env python3
import sys
import os
import time


# ── CONSTANTS FOR PAGE/RECORD LAYOUT ─────────────────────────────────────

NUM_SLOTS = 10
# Page header = 1 byte (occupied_count) + NUM_SLOTS bytes (slot bitmap: 0=free,1=occupied)
PAGE_HEADER_SIZE = 1 + NUM_SLOTS

# Pack/unpack helper: convert an integer to 4 bytes (big‐endian), and back
def int_to_bytes(x: int) -> bytes:
    return x.to_bytes(4, byteorder="big", signed=True)

def bytes_to_int(b: bytes) -> int:
    return int.from_bytes(b, byteorder="big", signed=True)

MAX_PAGES = 1000


CATALOG_FILE = "catalog.meta"
OUTPUT_FILE = "output.txt"
LOG_FILE = "log.csv"



def read_catalog():
    """
    Returns a dict:
    {
      "Person": {
          "num_fields": 3,
          "pk_index": 1,
          "fields": [ ("name","str",20), ("age","int",4), ("city","str",20) ]
      },
      ...
    }
    Malformed lines in catalog.meta are skipped.
    """
    catalog = {}
    with open(CATALOG_FILE, "r") as f:
        for lineno, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            parts = line.split("|")
            # we need at least: tname, nf, pk
            if len(parts) < 3:
                # malformed: not enough parts
                continue

            try:
                tname, nf_str, pk_str, *flds = parts
                nf = int(nf_str)
                pk = int(pk_str)
                fields = []
                for fld in flds:
                    fname, ftype, flen_str = fld.split(",")
                    flen = int(flen_str)
                    fields.append((fname, ftype, flen))

                # sanity-check: number of parsed fields matches nf
                if len(fields) != nf:
                    continue

                catalog[tname] = {
                    "num_fields": nf,
                    "pk_index": pk,
                    "fields": fields
                }

            except (ValueError, IndexError):
                # any parse error (bad int, wrong commas, etc.) → skip line
                continue

    return catalog


def write_catalog_entry(tname, num_fields, pk_index, fields_list):
    """
    fields_list = [("name","str",20), ("age","int",4), ...]
    Append one line to catalog.meta
    """
    entry = "|".join([
        tname,
        str(num_fields),
        str(pk_index),
    ] + [f"{fname},{ftype},{flen}" for fname, ftype, flen in fields_list])
    with open(CATALOG_FILE, "a") as f:
        f.write(entry + "\n")






def compute_record_size(fields: list) -> int:
    """
    Given fields = [("name","str",20), ("age","int",4), ...],
    returns total bytes per record = 1 (valid‐flag) + sum(flen).
    """
    total = 1  # 1 byte for valid‐flag
    for _, _, flen in fields:
        total += flen
    return total

def pack_record(fields: list, values: list) -> bytes:
    """
    fields: [("name","str",20), ("age","int",4), ...]
    values: ["Alice", "30", "Istanbul"]
    Returns a bytes object of length record_size.
    First byte = 1 (occupied). Then each field in order:
      - if int: 4‐byte big‐endian signed
      - if str: UTF‐8, then pad with b'\x00' up to flen
    """
    record_bytes = bytearray()
    record_bytes.append(1)  # valid = 1

    for (fname, ftype, flen), val in zip(fields, values):
        if ftype == "int":
            # Convert string→int→4 bytes
            try:
                ival = int(val)
            except ValueError:
                raise ValueError(f"Field '{fname}' expects int, got '{val}'")
            record_bytes.extend(int_to_bytes(ival))
        else:  # ftype == "str"
            s = val.encode("utf-8")
            if len(s) > flen:
                # truncate if longer than flen
                s = s[:flen]
            record_bytes.extend(s)
            # pad with nulls
            record_bytes.extend(b'\x00' * (flen - len(s)))

    return bytes(record_bytes)

def unpack_record(fields: list, record_bytes: bytes) -> list:
    """
    Given fields and a raw record_bytes (valid‐flag + field‐bytes),
    return a list of string representations: [ "Alice", "30", "Istanbul" ].
    (We ignore the valid‐flag byte, assuming it’s already checked.)
    """
    values = []
    offset = 1  # skip valid‐flag
    for (fname, ftype, flen) in fields:
        chunk = record_bytes[offset: offset + flen]
        if ftype == "int":
            ival = bytes_to_int(chunk)
            values.append(str(ival))
        else:
            # decode up to first null (b'\x00')
            s = chunk.rstrip(b'\x00').decode("utf-8", errors="ignore")
            values.append(s)
        offset += flen
    return values



def find_record_page_slot(tname: str, pk_value: str):
    """
    Scan <tname>.dat page by page searching for a record whose primary‐key field equals pk_value.
    Returns (page_offset, slot_index) if found, else (None, None).
    - page_offset = byte offset in file where the page starts.
    - slot_index ∈ [0 .. NUM_SLOTS-1].
    """
    catalog = read_catalog()
    if tname not in catalog:
        return None, None

    fields = catalog[tname]["fields"]
    pk_idx = catalog[tname]["pk_index"] - 1  # zero-based
    rec_size = compute_record_size(fields)
    page_size = PAGE_HEADER_SIZE + NUM_SLOTS * rec_size

    dat_filename = f"{tname}.dat"
    if not os.path.isfile(dat_filename):
        return None, None

    with open(dat_filename, "rb") as f:
        page_offset = 0
        while True:
            header = f.read(PAGE_HEADER_SIZE)
            if not header or len(header) < PAGE_HEADER_SIZE:
                break  # no more pages
            occupied_count = header[0]
            slot_bitmap = header[1:1+NUM_SLOTS]

            # If no records in this page, skip directly
            if occupied_count == 0:
                page_offset += page_size
                f.seek(page_offset)
                continue

            # Otherwise, read all slots
            for slot_idx in range(NUM_SLOTS):
                if slot_bitmap[slot_idx] == 1:
                    # This slot is occupied; read that record
                    record_offset = page_offset + PAGE_HEADER_SIZE + slot_idx * rec_size
                    f.seek(record_offset)
                    record_bytes = f.read(rec_size)
                    # Unpack to get fields; then compare primary key
                    values = unpack_record(fields, record_bytes)
                    if values[pk_idx] == pk_value:
                        return page_offset, slot_idx

            # Advance to next page
            page_offset += page_size
            f.seek(page_offset)

    return None, None

def find_free_slot_in_page(f_handle, page_offset: int, fields: list):
    """
    Given an open file handle positioned anywhere, and a page_offset,
    returns the first free slot_index ∈ [0..NUM_SLOTS-1], or None if page is full.
    """
    rec_size = compute_record_size(fields)
    page_size = PAGE_HEADER_SIZE + NUM_SLOTS * rec_size

    # Read header
    f_handle.seek(page_offset)
    header = f_handle.read(PAGE_HEADER_SIZE)
    if len(header) < PAGE_HEADER_SIZE:
        return None

    occupied_count = header[0]
    if occupied_count >= NUM_SLOTS:
        return None  # page is full

    slot_bitmap = header[1:1+NUM_SLOTS]
    for idx in range(NUM_SLOTS):
        if slot_bitmap[idx] == 0:
            return idx
    return None




def log_operation(op_string: str, status: bool):
    ts = int(time.time())
    # Append a line to log.csv: timestamp, the raw operation, and “success”/“failure”
    with open(LOG_FILE, "a") as log_f:
        log_f.write(f"{ts}, {op_string}, {'success' if status else 'failure'}\n")

def handle_create_type(tokens: list):
    """
    Expected tokens format:
      ["create", "type", type_name, num_fields, pk_index,
       field1_name, field1_type, field2_name, field2_type, ..., fieldN_name, fieldN_type]

    Returns True if type was created successfully; False otherwise.
    """
    op_string = " ".join(tokens)
    # Basic token‐count check: we need at least 6 tokens (create, type, name, nf, pk, first-field)
    if len(tokens) < 6:
        log_operation(op_string, False)
        return False

    # 1) Parse the fixed‐position arguments
    _, _, tname, nf_str, pk_str, *field_tokens = tokens

    # 2) Convert num_fields and pk_index to integers
    try:
        nf = int(nf_str)
        pk_index = int(pk_str)  # 1-based index of which field is the primary key
    except ValueError:
        log_operation(op_string, False)
        return False

    # 3) Validate basic constraints on nf and pk_index
    if nf < 1 or nf > 6:
        # must have between 1 and 6 fields
        log_operation(op_string, False)
        return False
    if pk_index < 1 or pk_index > nf:
        # primary key index must be within [1..nf]
        log_operation(op_string, False)
        return False

    # 4) Check type‐name length
    if len(tname) > 12:
        log_operation(op_string, False)
        return False

    # 5) Ensure we have exactly nf * 2 tokens for "field-name field-type"
    if len(field_tokens) != nf * 2:
        log_operation(op_string, False)
        return False

    # 6) Load existing catalog and check for duplicate type
    catalog = read_catalog()
    if tname in catalog:
        # type already exists
        log_operation(op_string, False)
        return False

    # 7) Validate each (field-name, field-type) pair
    fields_list = []  # will store tuples: (fname, ftype, flen)
    for i in range(nf):
        fname = field_tokens[2*i]
        ftype = field_tokens[2*i + 1]

        # a) field-name <= 20 chars
        if len(fname) > 20:
            log_operation(op_string, False)
            return False

        # b) field-type must be "int" or "str"
        if ftype not in ("int", "str"):
            log_operation(op_string, False)
            return False

        # c) assign a fixed byte‐length
        if ftype == "int":
            flen = 4
        else:  # ftype == "str"
            flen = 20  # choose 20 bytes max for every string field

        fields_list.append((fname, ftype, flen))

    # 8) All validations passed → write a new catalog entry
    #    We store pk_index exactly as provided (1-based).
    write_catalog_entry(tname, nf, pk_index, fields_list)

    # 9) Create an empty .dat file for this type. We'll leave pages to be created on first insertion.
    dat_filename = f"{tname}.dat"
    try:
        open(dat_filename, "wb").close()
    except OSError:
        # Failed to create the .dat file
        log_operation(op_string, False)
        return False

    # 10) Log success and return True
    log_operation(op_string, True)
    return True


def handle_create_record(tokens: list):
    """
    Expected tokens format:
      ["create", "record", type_name, val1, val2, ..., valN]
    Returns True if insertion succeeded; False otherwise.
    """
    op_string = " ".join(tokens)

    # 1) Minimal token‐count check
    if len(tokens) < 4:
        log_operation(op_string, False)
        return False

    _, _, tname, *value_tokens = tokens

    # 2) Load catalog; verify type exists
    catalog = read_catalog()
    if tname not in catalog:
        log_operation(op_string, False)
        return False

    # 3) Extract schema details
    num_fields = catalog[tname]["num_fields"]
    pk_index = catalog[tname]["pk_index"] - 1  # zero-based
    fields = catalog[tname]["fields"]
    rec_size = compute_record_size(fields)
    page_size = PAGE_HEADER_SIZE + NUM_SLOTS * rec_size
    dat_filename = f"{tname}.dat"

    # 4) Check correct number of values
    if len(value_tokens) != num_fields:
        log_operation(op_string, False)
        return False

    # 5) Check primary‐key uniqueness
    pk_value = value_tokens[pk_index]
    page_off, slot_idx = find_record_page_slot(tname, pk_value)
    if page_off is not None:
        # Duplicate primary key
        log_operation(op_string, False)
        return False

    # 6) Pack record bytes
    try:
        record_bytes = pack_record(fields, value_tokens)
    except ValueError:
        # Type‐mismatch in one of the fields
        log_operation(op_string, False)
        return False

    # 7) Open the .dat file (create if missing)
    #    We’ll append pages if no existing page has a free slot.
    fmode = "r+b" if os.path.isfile(dat_filename) else "w+b"
    with open(dat_filename, fmode) as f:
        # Pointer to first page
        page_offset = 0
        f.seek(0, os.SEEK_END)
        file_size = f.tell()
        f.seek(0)

        # If file is empty (no pages), create page0
        if file_size == 0:
            # Initialize a brand‐new empty page:
            #   occupied_count = 0, slot_bitmap = [0]*NUM_SLOTS, then zero out all slots
            empty_header = bytearray([0] + [0]*NUM_SLOTS)
            f.write(empty_header)
            f.write(b'\x00' * (NUM_SLOTS * rec_size))
            file_size = page_size

        # Now scan existing pages for a free slot
        while page_offset < file_size:
            free_slot = find_free_slot_in_page(f, page_offset, fields)
            if free_slot is not None:
                # We found a free slot here → write record
                #  a) update valid‐flag and field‐bytes
                record_offset = page_offset + PAGE_HEADER_SIZE + free_slot * rec_size
                f.seek(record_offset)
                f.write(record_bytes)

                #  b) update header: occupied_count + slot_bitmap
                f.seek(page_offset)
                header = bytearray(f.read(PAGE_HEADER_SIZE))
                header[0] += 1              # increment occupied_count
                header[1 + free_slot] = 1   # mark this slot occupied
                f.seek(page_offset)
                f.write(header)
                log_operation(op_string, True)
                return True

            # Move to next page
            page_offset += page_size

        # If we reach here, no existing page had room → append a new page
        f.seek(file_size)  # move to EOF


        # —— enforce your chosen pages-per-file cap ——
        num_pages = file_size // page_size
        if num_pages >= MAX_PAGES:
            log_operation(op_string, False)
            return False
        # ————————————————————————————————


        # Initialize new page (same as above)
        new_header = bytearray([0] + [0]*NUM_SLOTS)
        new_header[0] = 1                   # will store 1 occupied slot
        new_header[1 + 0] = 1               # we’ll insert in slot 0
        f.write(new_header)
        # Write the first record in slot 0
        #   pad the rest of the page’s slots with zero‐bytes
        page_content = bytearray(NUM_SLOTS * rec_size)
        page_content[0:rec_size] = record_bytes
        f.write(bytes(page_content))

        log_operation(op_string, True)
        return True

    # Shouldn’t get here, but if we do:
    log_operation(op_string, False)
    return False


def handle_delete_record(tokens: list):
    """
    Expected tokens format:
      ["delete", "record", type_name, primary_key_value]

    If found:
      - Mark that record’s valid‐flag = 0
      - Decrement the page’s occupied_count and clear its slot‐bitmap bit
      - Return True

    If not found or on error:
      - Return False
    """
    op_string = " ".join(tokens)

    # 1) Token‐count check: must be exactly 4 tokens
    if len(tokens) != 4:
        log_operation(op_string, False)
        return False

    _, _, tname, pk_value = tokens

    # 2) Load catalog; verify type exists
    catalog = read_catalog()
    if tname not in catalog:
        log_operation(op_string, False)
        return False

    # 3) Extract schema details
    fields = catalog[tname]["fields"]
    num_fields = catalog[tname]["num_fields"]
    pk_index = catalog[tname]["pk_index"] - 1  # zero-based
    rec_size = compute_record_size(fields)
    page_size = PAGE_HEADER_SIZE + NUM_SLOTS * rec_size
    dat_filename = f"{tname}.dat"

    # 4) Find the record’s page + slot
    page_off, slot_idx = find_record_page_slot(tname, pk_value)
    if page_off is None:
        # Record not found → failure
        log_operation(op_string, False)
        return False

    # 5) Open .dat in read‐write mode and update:
    try:
        with open(dat_filename, "r+b") as f:
            # (a) Invalidate the record: set its valid‐flag byte = 0
            record_offset = page_off + PAGE_HEADER_SIZE + slot_idx * rec_size
            f.seek(record_offset)
            f.write(b'\x00')  # write a single zero byte for valid‐flag

            # (b) Update page header: decrement occupied_count, clear slot bitmap
            f.seek(page_off)
            header = bytearray(f.read(PAGE_HEADER_SIZE))
            # header[0] is occupied_count; header[1 + slot_idx] is the bitmap
            if header[0] > 0:
                header[0] -= 1
            header[1 + slot_idx] = 0

            f.seek(page_off)
            f.write(header)
    except Exception:
        log_operation(op_string, False)
        return False

    # 6) Log success and return True
    log_operation(op_string, True)
    return True


def handle_search_record(tokens: list):
    """
    Expected tokens:
      ["search", "record", type_name, primary_key_value]
    If found: return list_of_field_values.
    If not found or any error: return None.
    """
    op_string = " ".join(tokens)

    # 1) Token‐count check
    if len(tokens) != 4:
        log_operation(op_string, False)
        return None

    _, _, tname, pk_value = tokens

    # 2) Verify type exists
    catalog = read_catalog()
    if tname not in catalog:
        log_operation(op_string, False)
        return None

    # 3) Schema details
    fields = catalog[tname]["fields"]
    rec_size = compute_record_size(fields)

    # 4) Locate the record
    page_off, slot_idx = find_record_page_slot(tname, pk_value)
    if page_off is None:
        log_operation(op_string, False)
        return None

    # 5) Read and unpack
    dat_filename = f"{tname}.dat"
    try:
        with open(dat_filename, "rb") as f:
            record_offset = page_off + PAGE_HEADER_SIZE + slot_idx * rec_size
            f.seek(record_offset)
            record_bytes = f.read(rec_size)
            values = unpack_record(fields, record_bytes)
    except Exception:
        log_operation(op_string, False)
        return None

    # 6) Found—log success (but do NOT write to output.txt here)
    log_operation(op_string, True)
    return values



def main():
    if len(sys.argv) != 2:
        print("Usage: python3 archive.py path/to/input.txt")
        sys.exit(1)

    input_path = sys.argv[1]
    if not os.path.isfile(input_path):
        print(f"Error: {input_path} not found.")
        sys.exit(1)

    # Ensure catalog.meta exists
    if not os.path.isfile(CATALOG_FILE):
        open(CATALOG_FILE, "w").close()

    # At program start, clear any existing output and log
    open(OUTPUT_FILE, "w").close()

    with open(input_path, "r") as fin, open(OUTPUT_FILE, "a") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue

            # Wrap *every* command in try/except:
            try:
                tokens = line.split()
                op = tokens[0].lower()

                if op == "create" and tokens[1].lower() == "type":
                    _ = handle_create_type(tokens)

                elif op == "create" and tokens[1].lower() == "record":
                    _ = handle_create_record(tokens)

                elif op == "delete" and tokens[1].lower() == "record":
                    _ = handle_delete_record(tokens)

                elif op == "search" and tokens[1].lower() == "record":
                    values = handle_search_record(tokens)
                    if values is not None:
                        fout.write(" ".join(values) + "\n")

                else:
                    # unknown command → log failure
                    log_operation(line, False)

            except Exception:
                # catch everything, log as failure, and move on
                log_operation(line, False)
                continue

if __name__ == "__main__":
    main()
