# Copyright (c) 2021-2022 José Manuel Barroso Galindo <theypsilon@gmail.com>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# You can download the latest version of this tool from:
# https://github.com/MiSTer-devel/Downloader_MiSTer
import subprocess
from downloader.constants import default_curl_ssl_options


class CertificatesFix:
    def __init__(self, config, file_system, logger):
        self._config = config
        self._file_system = file_system
        self._logger = logger

    def fix_certificates_if_needed(self):
        curl_ssl = self._config['curl_ssl'].strip().lower()
        if curl_ssl != default_curl_ssl_options.strip().lower():
            return

        parts = curl_ssl.split()
        if len(parts) != 2 or parts[0] != '--cacert':
            return

        cacert_path = parts[1]
        if self._file_system.is_file(cacert_path):
            self._logger.debug('cacert file seems to be fine at %s.' % cacert_path)
            return

        self._logger.print('cacert file seems to be missing.')

        try:
            self._file_system.touch(cacert_path)
        except OSError as _:
            self._logger.print('ERROR: cacert path is invalid!')
            return

        self._logger.print('Downloading new cacert file...')

        result = self._download(cacert_path)
        if result.returncode != 0:
            self._logger.print('ERROR: Download failed! %d' % result.returncode)
            return

        self._logger.print('New cacert file has been installed at %s!' % cacert_path)
        self._logger.print()

    def _download(self, path):
        return subprocess.run(['curl', '--insecure', '-o', str(path), 'https://curl.se/ca/cacert.pem'], stderr=subprocess.STDOUT)
