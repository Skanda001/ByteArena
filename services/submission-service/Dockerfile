# Multi-stage Dockerfile for submission-service
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

# Copy source files and proto
COPY proto/ ./proto/
COPY packages/ ./packages/
COPY services/submission-service/ ./services/submission-service/

# Generate proto types and build typescript
RUN npm run proto:gen
RUN npm run build --workspace=@bytearena/shared
RUN npm run build --workspace=@bytearena/submission-service

# Production runtime stage
FROM node:20-alpine AS runner

WORKDIR /app

ENV NODE_ENV=production

# Copy root package files
COPY package*.json tsconfig.base.json ./
COPY packages/shared/package*.json ./packages/shared/
COPY services/submission-service/package*.json ./services/submission-service/

# Install only production dependencies
RUN npm ci --omit=dev

# Copy compiled files and proto definitions
COPY --from=builder /app/proto ./proto
COPY --from=builder /app/packages/shared/dist ./packages/shared/dist
COPY --from=builder /app/packages/shared/generated ./packages/shared/generated
COPY --from=builder /app/services/submission-service/dist ./services/submission-service/dist

# Use non-root user
USER node

EXPOSE 50051

CMD ["node", "services/submission-service/dist/server.js"]
