FROM node:16.14.0

WORKDIR /app

COPY package.json ./

RUN npm install --legacy-peer-deps
RUN npm install -g serve
COPY . .
RUN npm run build


CMD ["npm","run","serve"]
