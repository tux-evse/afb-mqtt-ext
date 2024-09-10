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

CMAKE_MINIMUM_REQUIRED(VERSION 3.6)
project(afb-mqtt)

set(CMAKE_INSTALL_SO_NO_EXE 0)

# Project Info
# ------------------
set(PROJECT_PRETTY_NAME "MQTT Extension for AFB-binder")
set(PROJECT_DESCRIPTION "Provide MQTT connectivity")
set(PROJECT_URL "https://github.com/Tux-EVSE/mqtt-ocpp-ext")
set(PROJECT_ICON "icon.jpg")
set(PROJECT_AUTHOR "Iot-Team")
set(PROJECT_AUTHOR_MAIL "secretariat@iot.bzh")
set(PROJECT_LICENSE "Apache-2")
set(PROJECT_LANGUAGES,"C")
set(PROJECT_VERSION 1.0)

include(FindPkgConfig)
pkg_check_modules(deps REQUIRED
    libafb>=5
    libafb-binder>=5
    libmosquitto
)

set(SRC ./src/mqtt.c)


add_library(afb-mqtt-ext SHARED ${SRC})

target_link_libraries(afb-mqtt-ext ${deps_LDFLAGS})