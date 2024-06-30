import traceback
import requests, csv, logging, time
import pandas as pd

from ratelimit import limits, sleep_and_retry
from multiprocessing.dummy import Pool

OUTPUT_CSV = 'total_tx_2023.csv'
POOL_SIZE = 3
CONTRACT_ADDR = 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t'
ADDR_FILE = 'new_random_addresses.csv'

logging.basicConfig(level=logging.INFO)

def read_addr_from_csv():
    addresses = pd.read_csv(ADDR_FILE)
    return addresses['Address']


@sleep_and_retry
@limits(calls=3, period=2)
def request_api(url):
    return requests.get(url).json()


def find_tx_2023(address):
    start_ts = 1672531200000
    end_ts = 1704067199000

    page_size = 50
    num_page = 0
    has_more = True
    results = []

    while has_more:
        logging.debug(f'{address}: request page {num_page}')

        url = f'https://apilist.tronscanapi.com/api/filter/trc20/transfers?limit={page_size}&start={num_page * page_size}&sort=-timestamp&count=true&filterTokenValue=0&relatedAddress={address}&start_timestamp={start_ts}&end_timestamp={end_ts}'

        try:
            response = request_api(url)

            txns = response.get('token_transfers')
            if txns is None:
                raise Exception(f'Token transfers not found: {response}')

            logging.debug(f'{address}: received {len(txns)} txns')
            results.extend(
                map(
                    lambda t: {
                        'id': t['transaction_id'],
                        'address': address,
                        'block': t['block'],
                        'timestamp': t['block_ts'] // 1000,
                        'from': t['from_address'],
                        'to': t['to_address'],
                        'quant': float(t['quant']) / 1000000,
                    },
                    filter(lambda t: t['contract_address'] == CONTRACT_ADDR, txns)
                )
            )

            has_more = (num_page + 1) * page_size < int(response.get('total', 0))
            num_page += 1
        except:
            print(traceback.format_exc())
            return None
        
    logging.info(f'{address}: completed with {len(results)} txns')
    return results


if __name__ == '__main__':
    addresses = read_addr_from_csv()

    num_processed = 0
    total_addr = len(addresses)

    with open(OUTPUT_CSV, 'w', newline='') as csvfile:
        field_names = ['id', 'address', 'block', 'timestamp', 'from', 'to', 'quant']
        writer = csv.DictWriter(csvfile, fieldnames=field_names)
        writer.writeheader()

        def process_addr(addr):
            global num_processed
            txns = find_tx_2023(addr)
            if txns is None:
                logging.error(f'Error occured in address {addr}')
                time.sleep(120)
            else:
                num_processed += 1
                logging.info(f'Progress {round(num_processed / total_addr * 100, 2)}%  ({num_processed}/{total_addr})')
                writer.writerows(txns)

        with Pool(POOL_SIZE) as pool:
            pool.map(process_addr, addresses)
        