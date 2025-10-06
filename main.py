from pathlib import Path
from parser import Parser
import argparse


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("filename")
    args = arg_parser.parse_args()
    file = Path(args.filename)
    parser = Parser(file)
    parser.parse()

    # IPs
    print("Все клиенты сервиса (IP-адреса):", parser.all_ips, "\nКоличество валидных IP-адресов:", len(parser.all_ips))
    invalid_ips = set()
    ports = set()
    for ip in parser.invalid_ips:
        invalid_split = ip.split(".")
        invalid_ip = '.'.join(invalid_split[0:3])
        ports.add(invalid_split[3])
        invalid_ips.add(invalid_ip)
    print("Количество невалидных IP-адресов:", len(parser.invalid_ips),
          "\nНевалидные IP-адреса с уникальными первыми тремя октетами:", invalid_ips,
          "\nКоличество невалидных IP-адресов с уникальными первыми тремя октетами:", len(invalid_ips),
          "\nПредположительные порты невалидных IP-адресов:", ports,
          "\nКоличество предположительных портов невалидных IP-адресов:", len(ports))

    # Requests
    most_popular_request = parser.most_popular_request()
    print("Самый популярный запрос:", most_popular_request, "\nКоличество запросов (самый популярный запрос):",
          parser.request_freq[most_popular_request] if most_popular_request else 0)

    # Words
    print("Среднее количество слов в запросах:", parser.average_words())

    # Time
    average_working_time, average_working_time_with_waiting = parser.average_times()
    print("Среднее время обработки запроса без учета ожидания в очереди (мс):", average_working_time,
          "\nСреднее время обработки запроса общее (мс):",
          average_working_time_with_waiting)
    max_work_time, max_total_time = parser.max_times()
    print("Максимальное время обработки запроса без учета ожидания в очереди (мс):", max_work_time,
          "\nМаксимальное время обработки запроса общее (мс):", max_total_time)

    # RPS
    print("Среднее количество запросов в секунду:", parser.rps())


if __name__ == "__main__":
    main()
