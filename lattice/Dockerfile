FROM moleculacorp/nodejs:latest as build
# make sure that your docker settings allow for at least like 4gb of ram, it
# takes a lot to build this
WORKDIR /lattice
COPY package.json ./
RUN apk update && apk upgrade yarn
RUN yarn install --network-timeout 100000

COPY . ./
RUN yarn build

FROM caddy
COPY --from=build /lattice/build /lattice
COPY Caddyfile /etc/caddy/Caddyfile
