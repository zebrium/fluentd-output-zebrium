#!/bin/sh

echo "Build Zebrium Fluentd output plugin for Kubernetes"
set -e

PLUGIN_NAME="fluent-plugin-zebrium_output"

echo "Building for tag $VERSION, modify .gemspec file..."

echo "Install bundler..."
bundle install

echo "Build gem $PLUGIN_NAME $VERSION..."
gem build $PLUGIN_NAME

echo "DONE"
