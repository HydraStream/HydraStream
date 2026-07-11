import asyncio
import cProfile

from hydrastream.main import async_main

default = {
    "links": None,
    "input": None,
    "typehash": "md5",
    "hash": None,
    "output": "download",
    "threads": None,
    "stream": False,
    "dry-run": False,
    "min-chunk-mb": 1,
    "stream-chunk-mb": 5,
    "buffer": None,
    "limit": None,
    "no-ui": False,
    "quiet": True,
    "json": False,
    "verify": True,
    "browser": "chrome120",
    "debug": False,
}


def run_profile() -> None:
    asyncio.run(
        async_main(
            links=[
                "https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/001/405/GCF_000001405.40_GRCh38.p14/GCF_000001405.40_GRCh38.p14_cds_from_genomic.fna.gz"
            ],
            input_file=None,
            stream=True,
            typehash="md5",
            checksum=None,
            threads=128,
            dry_run=False,
            min_chunk_size_mb=1,
            max_stream_chunk_size_mb=5,
            speed_limit=True,
            no_ui=False,
            quiet=True,
            output_dir="download",
            buffer_size_mb=None,
            json_logs=False,
            verify=True,
            impersonate="chrome120",
            debug=True,
        )
    )


if __name__ == "__main__":
    import cProfile

    # 1. Создаем объект профилировщика напрямую
    profiler = cProfile.Profile()

    print("Запуск профилирования... Дождитесь окончания или нажмите Ctrl+C (один раз).")
    try:
        # 2. Включаем сбор метрик
        profiler.enable()

        run_profile()

    except SystemExit:
        # Перехватываем sys.exit(), чтобы он не заблокировал запись файла
        print("\nПрограмма вызвала sys.exit(). Переходим к сохранению профиля...")
    except KeyboardInterrupt:
        print("\nПрофилирование прервано пользователем.")
    finally:
        # 3. Выключаем сбор метрик БЕЗ ИСКЛЮЧЕНИЙ
        profiler.disable()

        # 4. Принудительно сохраняем результат в файл в текущую директорию
        profiler.dump_stats("hydrastream.prof")
        print("Успешно! Файл 'hydrastream.prof' сохранен.")
