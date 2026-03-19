FROM apify/actor-node:20

WORKDIR /usr/src/app

COPY package*.json ./
RUN npm ci --include=dev

COPY tsconfig.json ./
COPY .actor ./.actor
COPY src ./src
COPY tests ./tests
COPY README.md ./README.md

RUN npm run build

ENV NODE_ENV=production
CMD ["npm", "start"]
