sed -i -e s/HOSTNAMEVAR/${HOSTNAMEVAR}/ /etc/pilosa.conf
pilosa -config /etc/pilosa.conf
