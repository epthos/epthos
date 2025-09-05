#!/bin/bash -e

if [ ! -d out ]; then
	mkdir out
fi

name=top
out="out/${name}"

if [ ! -f "${out}.crt" ]; then
	echo "Generating ${name}"

	openssl genpkey -out "${out}.key" -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -aes-128-cbc
	openssl req -new -config "${name}.cfg" -out "${out}.csr" -key "${out}.key"
	openssl x509 -req -days 4096 -in "${out}.csr" -signkey "${out}.key" -out "${out}.crt" -extfile "${name}.ext"
fi

name=int
out="out/${name}"

if [ ! -f "${out}.crt" ]; then
	echo "Generating ${name}"

	openssl req -nodes -newkey rsa:2048 -config "${name}.cfg" -out "${out}.csr" -keyout "${out}.key"
	openssl x509 -req -days 4096 -in "${out}.csr" -CA "out/top.crt" -CAkey "out/top.key" -CAcreateserial -out "${out}.crt" -extfile "${name}.ext"
fi

name=bob
out="out/${name}"

if [ ! -f "${out}.crt" ]; then
	echo "Generating ${name}"

	openssl req -nodes -newkey rsa:2048 -config "${name}.cfg" -out "${out}.csr" -keyout "${out}.key"
	openssl x509 -req -days 365 -in "${out}.csr" -CA "out/int.crt" -CAkey "out/int.key" -CAcreateserial -out "${out}.crt" -extfile "${name}.ext"
fi

name=broker
out="out/${name}"

if [ ! -f "${out}.crt" ]; then
	echo "Generating ${name}"

	openssl req -nodes -newkey rsa:2048 -config "${name}.cfg" -out "${out}.csr" -keyout "${out}.key"
	openssl x509 -req -days 365 -in "${out}.csr" -CA "out/int.crt" -CAkey "out/int.key" -CAcreateserial -out "${out}.crt" -extfile "${name}.ext"
fi

name=bobsource
out="out/${name}"

if [ ! -f "${out}.crt" ]; then
	echo "Generating ${name}"

	openssl req -nodes -newkey rsa:2048 -config "${name}.cfg" -out "${out}.csr" -keyout "${out}.key"
	openssl x509 -req -days 365 -in "${out}.csr" -CA "out/int.crt" -CAkey "out/int.key" -CAcreateserial -out "${out}.crt" -extfile "${name}.ext"
	cat "out/int.crt" >> ${out}.crt
fi

name=bobsink
out="out/${name}"

if [ ! -f "${out}.crt" ]; then
	echo "Generating ${name}"

	openssl req -nodes -newkey rsa:2048 -config "${name}.cfg" -out "${out}.csr" -keyout "${out}.key"
	openssl x509 -req -days 365 -in "${out}.csr" -CA "out/int.crt" -CAkey "out/int.key" -CAcreateserial -out "${out}.crt" -extfile "${name}.ext"
	cat "out/int.crt" >> ${out}.crt
fi

name=bobcli
out="out/${name}"

if [ ! -f "${out}.crt" ]; then
	echo "Generating ${name}"

	openssl req -nodes -newkey rsa:2048 -config "${name}.cfg" -out "${out}.csr" -keyout "${out}.key"
	openssl x509 -req -days 365 -in "${out}.csr" -CA "out/int.crt" -CAkey "out/int.key" -CAcreateserial -out "${out}.crt" -extfile "${name}.ext"
	cat "out/int.crt" >> ${out}.crt
fi

# Regenerate broker.toml's connection part
key=$(cat out/broker.key)
crt=$(cat out/broker.crt)
int=$(cat out/int.crt)
top=$(cat out/top.crt)

cat > out/broker.toml <<EOF
[connection]

peer_root = """
${top}
"""

certificate = """
${crt}
${int}
"""

private_key = """
${key}
"""

[server]
address = "[::0]:50001"

[process]
tracing_level = "INFO"
EOF

# Bob's source and sink can be generated using the CLI and the
# respective bobsource/bobsink files.
