FROM moleculacorp/nodejs:latest as build

WORKDIR /lattice

COPY package.json ./
COPY yarn.lock ./
RUN yarn install

COPY . ./
RUN yarn build

FROM caddy
COPY --from=build /lattice/build /lattice
COPY Caddyfile /etc/caddy/Caddyfile
