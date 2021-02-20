FROM node

ENV NODE_VERSION stable
ENV NPM_SCRIPT start

RUN git clone https://github.com/radiantearth/stac-browser.git

WORKDIR /stac-browser

RUN npm install -g parcel-bundler
RUN npm install -g n --silent

EXPOSE 1234

ENTRYPOINT ["npm", "start"]