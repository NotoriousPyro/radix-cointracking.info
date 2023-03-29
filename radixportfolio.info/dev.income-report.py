import csv
from itertools import groupby
from operator import itemgetter
from os.path import dirname, abspath
from typing import Any, Generator

IN = f"{dirname(abspath(__file__))}/radix-rewards-raw.csv"
OUT = f"{dirname(abspath(__file__))}/dev.out.csv"
CURRENCIES = ['gbp']
TOKEN_IDENTIFIERS = {
    'xrd_rr1qy5wfsfh': 'EXRD'
}


# dev.RadixPortfolio.info Income Report -> CoinTracking.info Custom Exchange importer CSV transformer
with open(IN, newline='', encoding='UTF-8-sig') as csvfile:
    rewardDate = itemgetter('rewardDate')
    rewardDate_validator_tokenIdentifier = itemgetter('rewardDate', 'validator', 'tokenIdentifier')
    reader = csv.DictReader(csvfile, delimiter=',', doublequote=True)
    out_field_names = [
        field for field
        in reader.fieldnames
        if field in [
            'rewardDate',
            'validator',
            'tokenIdentifier',
            'reward',
            *CURRENCIES
        ]
    ]
    out_field_names.append('epochRange')
    def duplicate_groups() -> Generator[dict[str, str] | list[dict[str, str]], None, None]:
        # Group duplicates
        for group in groupby(sorted(reader, key=rewardDate_validator_tokenIdentifier), key=rewardDate_validator_tokenIdentifier):
            gs = [*group[1]]
            if (len(gs) < 2):
                yield gs[0]
            else:
                yield gs
    def fixed_rows() -> Generator[dict[str, str], None, None]:
        # Combine duplicated rows (same date and validator into one row)
        for duplicate in duplicate_groups():
            start = itemgetter(0)(duplicate)
            start['rewardDate'] = f"{start['rewardDate']} 23:59:59"
            start['reward'] = str(float(start['reward']))
            if len(duplicate) < 2:
                yield start
            last = itemgetter(len(duplicate)-1)(duplicate)
            start['epochRange'] = f"{start['epoch']}-{last['epoch']}"
            start['tokenIdentifier'] = TOKEN_IDENTIFIERS.get(start['tokenIdentifier'], start['tokenIdentifier'])
            for index in list(range(1, len(duplicate))):
                other = itemgetter(index)(duplicate)
                if int(other['epoch']) not in range(int(start['epoch']), int(last['epoch'])+1):
                    raise Exception("Discontiguous epochs")
                start['reward'] = str(float(start['reward']) + float(other['reward']))
            yield start
    # Write the CSV
    with open(OUT, mode='w', encoding='UTF-8-sig', newline='') as csvout:
        writer = csv.DictWriter(csvout, fieldnames=out_field_names, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(fixed_rows())
