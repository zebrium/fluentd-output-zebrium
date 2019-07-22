#!/bin/sh

. /auto/share/etc/functions

PROG=${0##*/}

PLUGIN_NAME="fluent-plugin-zebrium_output"

main() {
    echo "Build Zebrium Fluentd output plugin for Kubernetes"
    gem build $PLUGIN_NAME
    echo "DONE"
}

main "$@"
