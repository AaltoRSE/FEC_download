import argparse
from FECdownload import fec_scheduleA_year_range


def main():
    parser = argparse.ArgumentParser(description="Fetch data from FEC API")
    parser.add_argument("-k", "--api_key", metavar="", default="DEMO_KEY", help="your FEC API key, default: DEMO_KEY")
    parser.add_argument("-s", "--start", metavar="", default=1990, help="The first year for which data is requested. The data is returned in 2 year chunks and earlier data may be returned.")
    parser.add_argument("-e", "--end", metavar="", default=2025, help="The last year for which data is requested. The data is returned in 2 year chunks and later data may be returned.")
    parser.add_argument("-E", "--employer", metavar="", default=None, help="The employer for which data is requested. If not provided, all employers are requested.")
    parser.add_argument("-o", "--output", metavar="", default=None, help="Output file name. Default: fec_scheduleA_[EMPLOYER_]START_END.json")
    args = parser.parse_args()

    if args.api_key == "DEMO_KEY":
        print("Warning: Using DEMO_KEY. This API key is rate-limited and should not be used for production. Get a personal key at https://api.data.gov/signup.")
    
    start = int(args.start)
    end = int(args.end)

    if args.output is None:
        if args.employer is not None:
            output_filename = f"fec_scheduleA_{args.employer}_{start}_{end}.json"
        else:
            output_filename = f"fec_scheduleA_{start}_{end}.json"
    else:
        output_filename = args.output

    data = fec_scheduleA_year_range(start, end, args.api_key, args.employer)
    data.to_csv(output_filename)


if __name__ == "__main__":
    main()
