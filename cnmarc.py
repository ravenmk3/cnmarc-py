import io
import typing
import pydantic

CHAR_LF = 10
CHAR_CR = 13
RECORD_SEP = 29
FIELD_SEP = 30
SUBFIELD_SEP = 31
SUBFIELD_SEP_BIN = b'\x1f'
CR_LF_BIN = b'\r\n'
ENCODING = 'ascii'
DATA_ENCODING = 'gb18030'
LEADER_LENGTH = 24
DIRECTORY_ENTRY_LENGTH = 12


class Leader(pydantic.BaseModel):
    record_length: int
    data_index: int


class DirectoryEntry(pydantic.BaseModel):
    tag: str
    length: int
    index: int


class SubField(pydantic.BaseModel):
    tag: str
    value: str


class Field(pydantic.BaseModel):
    tag: str
    value: str | None
    fields: list[SubField] | None


class Record(pydantic.BaseModel):
    leader: Leader
    directory: list[DirectoryEntry] | None
    fields: list[Field] | None


def parse_leader(buffer: bytes) -> Leader:
    assert len(buffer) == LEADER_LENGTH, 'invalid length of leader data'
    text = buffer.decode(ENCODING)
    length_str = text[0:5]
    length = int(length_str)
    data_index_str = text[12:17]
    data_index = int(data_index_str)
    lbl = Leader(record_length=length, data_index=data_index)
    return lbl


def parse_directory(buffer: bytes) -> list[DirectoryEntry]:
    buf_len = len(buffer)
    assert buf_len % DIRECTORY_ENTRY_LENGTH == 0, 'invalid length of directory data'
    entry_count = buf_len // DIRECTORY_ENTRY_LENGTH
    text = buffer.decode(ENCODING)
    entries = []

    for i in range(entry_count):
        offset = i * DIRECTORY_ENTRY_LENGTH
        field_tag = text[offset: offset + 3]
        field_len = text[offset + 3: offset + 7]
        field_idx = text[offset + 7: offset + 12]
        entry = DirectoryEntry(tag=field_tag, length=int(field_len), index=int(field_idx))
        entries.append(entry)

    return entries


def parse_fields(buffer: bytes, dir: list[DirectoryEntry]) -> list[Field]:
    fields = []
    for entry in dir:
        value_buf = buffer[entry.index: entry.index + entry.length - 1]
        sub_buffers = value_buf.split(SUBFIELD_SEP_BIN)
        value = sub_buffers[0].decode(DATA_ENCODING)
        sub_fields = []
        for n in range(1, len(sub_buffers)):
            sub_buf = sub_buffers[n]
            sub_tag = sub_buf[0:1].decode(DATA_ENCODING)
            sub_value = sub_buf[1:].decode(DATA_ENCODING)
            sub_fields.append(SubField(tag=sub_tag, value=sub_value))
        field = Field(tag=entry.tag, value=value, fields=sub_fields)
        fields.append(field)
    return fields


def parse_record(buffer: bytes) -> Record:
    leader_buf = buffer[:LEADER_LENGTH]
    leader = parse_leader(leader_buf)

    dir_buf = buffer[LEADER_LENGTH:leader.data_index - 1]
    directory = parse_directory(dir_buf)

    fields_buf = buffer[leader.data_index:]
    fields = parse_fields(fields_buf, directory)

    return Record(leader=leader, directory=directory, fields=fields)


def find_char(buffer: bytes, char: int) -> int:
    for i in range(len(buffer)):
        if buffer[i] == char:
            return i
    return -1


def read_records(input: typing.IO) -> list[Record]:
    block_size = 24
    records = []
    buffer = b''

    while True:
        block = input.read(block_size)
        if not block:
            break

        i = find_char(block, RECORD_SEP)
        if i < 0:
            buffer += block
            continue

        record_data = buffer + block[:i + 1]
        buffer = block[i + 1:]

        record = parse_record(record_data.strip(CR_LF_BIN))
        if record:
            records.append(record)

    return records
