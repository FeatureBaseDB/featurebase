sed -i -e s/ZZZ/${DCHOSTNAME}/ /etc/pilosa.conf
pilosa -config /etc/pilosa.conf
