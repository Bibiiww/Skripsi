FROM node:22.13.1-alpine3.21 AS build
WORKDIR /app
COPY package.json pnpm-lock.yaml tsconfig.json ./
# Corepack bundled with some Node 22 images has an out-of-date signing keyring.
# Install the exact package-manager version declared in package.json instead.
RUN npm install --global pnpm@11.19.0 && pnpm install --frozen-lockfile --ignore-scripts
COPY src ./src
RUN npm run build

FROM node:22.13.1-alpine3.21
WORKDIR /app
ENV NODE_ENV=production
COPY package.json pnpm-lock.yaml ./
RUN npm install --global pnpm@11.19.0 && pnpm install --prod --frozen-lockfile --ignore-scripts
COPY --from=build /app/dist ./dist
EXPOSE 3000
CMD ["node", "dist/server.js"]
