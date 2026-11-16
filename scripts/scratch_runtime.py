# This software is in the public domain under CC0 1.0 Universal plus a
# Grant of Patent License.
#
# To the extent possible under law, the author(s) have dedicated all
# copyright and related and neighboring rights to this software to the
# public domain worldwide. This software is distributed without any
# warranty.
#
# You should have received a copy of the CC0 Public Domain Dedication
# along with this software (see the LICENSE.md file). If not, see
# <http://creativecommons.org/publicdomain/zero/1.0/>.

"""A scratch MR-Moqui runtime with the framework's base components and conf, and moqui-math, moqui-device and this
component as its only add-ons (runtime/components, plans/archive/Components-Dir.md of MR-Moqui)."""
import pathlib

ADDONS = ("moqui-math", "moqui-device", "moqui-device-gateway")
COMPONENTS_XML = """<?xml version="1.0" encoding="UTF-8" ?>
<components>
    <component-dir location="base"/>
    <component-dir location="addon"/>
</components>
"""

def scratch_runtime(root, framework):
    runtime = pathlib.Path(root)/"runtime"
    components = runtime/"components"
    (components/"addon").mkdir(parents=True)
    (components/"components.xml").write_text(COMPONENTS_XML)
    (components/"base").symlink_to(framework/"runtime/components/base", target_is_directory=True)
    (runtime/"conf").symlink_to(framework/"runtime/conf", target_is_directory=True)
    for name in ADDONS:
        (components/"addon"/name).symlink_to(framework/"runtime/components/addon"/name, target_is_directory=True)
    return runtime
