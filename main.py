import json

import cnmarc


def main():
    with open('sample.iso', 'rb') as fp:
        records = cnmarc.read_records(fp)
        print(records)


if __name__ == '__main__':
    main()
