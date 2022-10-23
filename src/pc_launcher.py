import os
import subprocess
from shutil import copyfileobj
from tempfile import NamedTemporaryFile
from urllib.request import urlopen


temp_container = {}


def fetch_temp_downloader():
    with NamedTemporaryFile(suffix='.zip', mode='wb', delete=False) as temp:
        temp_container['downloader'] = temp
        with urlopen('https://github.com/theypsilon-test/downloader/releases/download/latest/downloader.zip') as in_stream:
            if in_stream.status == 200:
                copyfileobj(in_stream, temp)


def launch_downloader(filename):
    env = os.environ.copy()
    env['PC_LAUNCHER'] = os.path.realpath(__file__)
    return subprocess.run(['python3', filename], env=env, stderr=subprocess.STDOUT).returncode


def main():
    try:
        fetch_temp_downloader()
        result = launch_downloader(temp_container['downloader'].name)
    except Exception as e:
        print(e)
        result = 1

    if 'downloader' in temp_container:
        try:
            os.unlink(temp_container['downloader'].name)
        except FileNotFoundError:
            pass

    input("Press Enter to continue...")

    return result


if __name__ == '__main__':
    exit(main())
