java -jar target/iri-1.8.1_original.jar -n tcp://192.168.0.101:15600 --testnet true --zmq-enable-tcp true \
	--remote true --remote-limit-api true \
	--testnet-coordinator JPDPNHABUWUFZSZWDGEYZBIGILECSAKBDZSEXE9GKNXMD9PAGMGSJTFUGGENFHJKOGWNTCZFFDLURUIET \
	--testnet-coordinator-security-level 1 \
	--testnet-coordinator-signature-mode CURLP27 \
	--mwm 9 \
	--milestone-start 0 \
	--milestone-keys 16 \
	--snapshot snapshot.txt \
	--max-depth 1000
