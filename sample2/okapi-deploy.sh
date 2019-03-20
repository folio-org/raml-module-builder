#!/bin/sh
curl -d@ModuleDescriptor.json http://localhost:9130/_/proxy/modules
curl -d'{"id":"testlib"}' http://localhost:9130/_/proxy/tenants
curl -d'[{"id":"mod-sample2-1.0.0", "action":"enable"}]' "http://localhost:9130/_/proxy/tenants/testlib/install?deploy=true"
