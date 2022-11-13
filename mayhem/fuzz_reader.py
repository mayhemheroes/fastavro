#!/usr/bin/env python3
import atheris
import io
import sys

with atheris.instrument_imports():
    import fastavro as fa

value_error_matches = [
    'avro',
    'sync marker'
]

custom_exceptions = (fa.validation.ValidationError, fa.read.SchemaResolutionError,
                     fa.schema.SchemaParseException,)


@atheris.instrument_func
def TestOneInput(data):
    fdp = atheris.FuzzedDataProvider(data)
    try:
        with io.BytesIO(fdp.ConsumeBytes(fdp.remaining_bytes())) as avro_file:
            for record in fa.reader(avro_file):
                dir(record)
    except custom_exceptions:
        return -1
    except ValueError as e:
        if any(e in value_error_matches for e in value_error_matches):
            return -1
        raise e
    except (IndexError, KeyError, EOFError,):
        # These are valid errors, just raised too quickly
        return -1


def main():
    atheris.Setup(sys.argv, TestOneInput)
    atheris.Fuzz()


if __name__ == '__main__':
    main()
