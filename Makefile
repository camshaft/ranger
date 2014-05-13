PROJECT = ranger

DEPS = fast_key gun

dep_fast_key = https://github.com/camshaft/fast_key.git master
dep_gun = pkg://gun master

include erlang.mk
