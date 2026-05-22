FROM node:18-alpine
WORKDIR /app
COPY package.json ./
RUN npm install --production
COPY index.js ./
COPY public ./public
CMD ["node", "index.js"]
