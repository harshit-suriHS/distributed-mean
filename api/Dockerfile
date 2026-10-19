FROM node:24-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY tsconfig.json tsconfig.build.json ./
COPY src ./src
RUN npm run build

FROM node:24-alpine
WORKDIR /app
ENV NODE_ENV=production
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY --from=build /app/dist ./dist
COPY proto ./proto
USER node
EXPOSE 3000
HEALTHCHECK --interval=5s --timeout=3s --retries=10 \
  CMD wget -qO- "http://127.0.0.1:${API_PORT:-3000}/health" || exit 1
CMD ["node", "dist/index.js"]
