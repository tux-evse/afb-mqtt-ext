# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] - 2026-04-09

### Changed

- (breaking) Change the variable syntax in templates from '${...}' to '%{...}' so that it is different from the AFB template syntax
- (breaking) Move configuration from an external YAML file to an internal configuration

### Added

- Add an "extended" publish mode where the verb name is added to the publish topic
- Allows one to specify the MQTT client id

### Fixed

- Make sure subscriptions are made after the client is actually connected to the MQTT broker

## [1.0.0] - 2026-04-08

 - Fix compiling and installing
 - Initial changelog
