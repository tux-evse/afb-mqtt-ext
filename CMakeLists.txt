###########################################################################
# Copyright 2015, 2016, 2017, 2018, 2019, 2024 IoT.bzh
#
# author: Hugo Mercier <hugo.mercier@iot.bzh>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
###########################################################################

CMAKE_MINIMUM_REQUIRED(VERSION 3.14)

project(afb-mqtt-ext
	VERSION 2.0.0
	DESCRIPTION "Provide MQTT connectivity"
	HOMEPAGE_URL "https://github.com/tux-evse/afb-mqtt-ext"
	LANGUAGES C)

include(FindPkgConfig)
include(GNUInstallDirs)

pkg_check_modules(deps REQUIRED
    libafb>=5
    libmosquitto
)

set(SRC ./src/mqtt.c ./src/json-utils.c)


add_library(afb-mqtt-ext SHARED ${SRC})

target_compile_options(afb-mqtt-ext PRIVATE -Wall ${deps_CFLAGS})

target_link_libraries(afb-mqtt-ext ${deps_LDFLAGS})

install(TARGETS afb-mqtt-ext
        LIBRARY
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/afb-ext
)
