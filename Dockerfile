FROM node:22-alpine AS base
RUN npm install -g pnpm@11.1.1

FROM base AS builder
WORKDIR /app

COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
RUN pnpm install --frozen-lockfile

COPY tsconfig.json tsconfig.app.json tsconfig.tools.json ./
COPY src ./src
RUN pnpm build

FROM base AS runner
WORKDIR /app
ENV NODE_ENV=production

COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
RUN pnpm install --frozen-lockfile --production

COPY --from=builder /app/dist ./dist
COPY drizzle ./drizzle

EXPOSE 3000
CMD ["node", "dist/server.js"]
