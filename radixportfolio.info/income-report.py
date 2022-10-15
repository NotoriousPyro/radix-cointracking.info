import csv
from itertools import groupby
from operator import itemgetter

# RadixPortfolio.info Income Report -> CoinTracking.info Custom Exchange importer CSV transformer
# Transforms the exported CSV:
# * Removes rows with dates of "up to"
# * Removes rows with 0 epoch lengths (no staking)
# * Removes rows with 0 rewards
# * Combines the values of duplicate rows for the same days
with open('in.csv', newline='', encoding='UTF-8-sig') as csvfile:
    rewardDate = itemgetter('rewardDate')
    rewardDate_validator = itemgetter('rewardDate', 'validator')
    reader = csv.DictReader(csvfile, delimiter=',', doublequote=True)
    # Remove rows with 0 reward, 0 epoch length, and dates starting with 'up'
    rows = sorted([
        row for row
        in reader
        if not row.get('rewardDate', {}).startswith('Up')
        and not row.get('dailyRewards', {}) == '0'
        and not row.get('epochsInDay', {}) == '0'
    ], key=rewardDate)
    # We only want a subset of fields
    out_field_names = [
        field for field
        in reader.fieldnames
        if field in [
            'rewardDate',
            'validator',
            'validatorName',
            'startEpoch',
            'endEpoch',
            'epochRange',
            'epochsInDay',
            'previousDayRewards',
            'endOfDayRewards',
            'dailyRewards'
        ]
    ]
    out_field_names.append('rewardCurrency')
    duplicates: list[dict[str, str]] = []
    nonduplicates: list[dict[str, str]] = []
    # Group duplicates using rewardDate_validator getter
    for group in groupby(sorted(rows, key=rewardDate_validator), key=rewardDate_validator):
        gs = [*group[1]]
        if (len(gs) < 2):
            nonduplicates.append(gs[0])
        else:
            duplicates.append(gs)
    # Combine duplicated rows (same date and validator into one row)
    for duplicate in duplicates:
        start = itemgetter(0)(duplicate)
        for index in list(range(1, len(duplicate))):
            other = itemgetter(index)(duplicate)
            if (str(int(start['endEpoch'])+1) != other['startEpoch']):
                raise Exception("Discontiguous epochs")
            start['endEpoch'] = other['endEpoch']
            start['epochsInDay'] = str(int(start['epochsInDay']) + int(other['epochsInDay']))
            start['dailyRewards'] = str(float(start['dailyRewards']) + float(other['dailyRewards']))
            start['endOfDayRewards'] = other['endOfDayRewards']
            start['epochRange'] = f'{start["startEpoch"]}-{start["endEpoch"]}'
        nonduplicates.append(start)
    # Add currency
    for row in nonduplicates:
        row['rewardCurrency'] = 'EXRD'
    # Write the CSV
    with open('out.csv', mode='w', encoding='UTF-8-sig', newline='') as csvout:
        writer = csv.DictWriter(csvout, fieldnames=out_field_names, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(sorted(
            [row for row in nonduplicates],
            key=rewardDate
        ))
