#!/usr/bin/env bash
# Copyright (c) 2021-2022 José Manuel Barroso Galindo <theypsilon@gmail.com>

set -euo pipefail

TEMP_ZIP1="$(mktemp -u).zip"
DOWNLOADER_ZIP="downloader.zip"
BIN="/tmp/dont_download.zip"
UUDECODE_CMD=$({ [[ "${MISTER:-false}" == "false" ]] && [[ "$(uname -s)" == "Darwin" ]] ; } && echo "uudecode -p" || echo "uudecode -o -")
EXPORTS="export COMMIT=$(git rev-parse --short HEAD)"

if [[ "${DEBUG:-false}" == "true" ]] ; then
  EXPORTS="${EXPORTS}"$'\n'"export DEBUG=true"
fi

pin_metadata() {
  touch -a -m -t 202108231405 "${1}"
}

cd src

find downloader -type f -iname "*.py" -print0 | while IFS= read -r -d '' file ; do pin_metadata "${file}" ; done
pin_metadata __main__.py
zip -q -0 -D -X -A -r "${TEMP_ZIP1}" __main__.py downloader -x "*/__pycache__/*"
pin_metadata "${TEMP_ZIP1}"
echo '#!/usr/bin/env python3' | cat - "${TEMP_ZIP1}" > "${DOWNLOADER_ZIP}"
pin_metadata "${DOWNLOADER_ZIP}"
rm "${TEMP_ZIP1}"
cd ..

cat <<-EOF
#!/usr/bin/env bash
set -euo pipefail
export DOWNLOADER_LAUNCHER_PATH="\${DOWNLOADER_LAUNCHER_PATH:-\${0}}"
${EXPORTS}
${UUDECODE_CMD} "\${0}" | xzcat -d -c > "${BIN}"
chmod a+x "${BIN}"
"${BIN}" "\${1:-}"
exit 0
EOF

uuencode - < <(xzcat -z < "${DOWNLOADER_ZIP}")

