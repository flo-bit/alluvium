#!/usr/bin/env bash
# Installs or updates Alluvium on a Debian/Ubuntu host. Run as root from a
# directory holding the two Linux binaries and this deploy/ folder:
#
#   ./deploy/install.sh ./jetstream ./alluvium
#
# Existing files under /etc/alluvium are left alone.
set -euo pipefail

jetstream_bin=${1:?path to the jetstream binary}
alluvium_bin=${2:?path to the alluvium binary}
here=$(cd "$(dirname "$0")" && pwd)

id alluvium >/dev/null 2>&1 || useradd --system --home /var/lib/alluvium --shell /usr/sbin/nologin alluvium
install -d -o alluvium -g alluvium /var/lib/alluvium /var/lib/alluvium/jetstream /var/lib/alluvium/snapshots /var/lib/alluvium/spill
install -d /opt/alluvium/bin /etc/alluvium
install -m 0755 "$jetstream_bin" /opt/alluvium/bin/jetstream.new
install -m 0755 "$alluvium_bin" /opt/alluvium/bin/alluvium.new
mv /opt/alluvium/bin/jetstream.new /opt/alluvium/bin/jetstream
mv /opt/alluvium/bin/alluvium.new /opt/alluvium/bin/alluvium

for f in jetstream.env alluvium.env; do
	[ -e "/etc/alluvium/$f" ] || install -m 0640 -g alluvium "$here/$f" "/etc/alluvium/$f"
done
# Jetstream needs the list to exist; discovery fills it.
[ -e /var/lib/alluvium/repos.txt ] || install -o alluvium -g alluvium -m 0644 /dev/null /var/lib/alluvium/repos.txt

install -m 0644 "$here"/alluvium-jetstream.service "$here"/alluvium.service \
	"$here"/alluvium-build.service "$here"/alluvium-build.timer \
	"$here"/alluvium-discover.service "$here"/alluvium-discover.timer /etc/systemd/system/
systemctl daemon-reload
echo "installed. Next: systemctl enable --now alluvium-jetstream alluvium alluvium-build.timer alluvium-discover.timer"
echo "The first snapshot: systemctl start --no-block alluvium-build"
