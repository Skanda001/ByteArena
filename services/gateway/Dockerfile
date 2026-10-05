# Multi-stage Dockerfile for gateway
# Build context: repo root

FROM node:20-alpine AS builder

WORKDIR /app

# Copy root and package configuration
COPY package*.json tsconfig.base.json ./
COPY packages/shared/package*.json ./packages/shared/
COPY services/submission-service/package*.json ./services/submission-service/
COPY services/runner/package*.json ./services/runner/
COPY services/gateway/package*.json ./services/gateway/

# Install all dependencies for build
RUN npm ci

# Copy source files, schema and proto
COPY proto/ ./proto/
COPY schema/ ./schema/
COPY packages/ ./packages/
COPY services/gateway/ ./services/gateway/

# Generate proto types and build typescript
RUN npm run proto:gen
RUN npm run build --workspace=@bytearena/shared
RUN npm run build --workspace=@bytearena/gateway

# Production runtime stage
FROM node:20-alpine AS runner

WORKDIR /app

ENV NODE_ENV=production

# Copy root package files
COPY package*.json tsconfig.base.json ./
COPY packages/shared/package*.json ./packages/shared/
COPY services/gateway/package*.json ./services/gateway/

# Install only production dependencies
RUN npm ci --omit=dev

# Copy compiled files, schema, and proto definitions
COPY --from=builder /app/proto ./proto
COPY --from=builder /app/schema ./schema
COPY --from=builder /app/packages/shared/dist ./packages/shared/dist
COPY --from=builder /app/packages/shared/generated ./packages/shared/generated
COPY --from=builder /app/services/gateway/dist ./services/gateway/dist

# Use non-root user
USER node

EXPOSE 4000

CMD ["node", "services/gateway/dist/index.js"]
