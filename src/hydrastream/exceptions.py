from enum import IntEnum


class ExitCode(IntEnum):
    SUCCESS = 0
    GENERAL_ERROR = 1  # Неизвестная критическая ошибка
    USAGE_ERROR = 2  # Юзер передал плохой путь к файлу (-i bad_file.txt)
    HASH_MISMATCH = 3  # Файл скачался, но MD5 не совпал
    NETWORK_ERROR = 4  # Отвалился интернет или сервер выдает 404/503
    INTERRUPTED = 130  # Пользователь нажал Ctrl+C
