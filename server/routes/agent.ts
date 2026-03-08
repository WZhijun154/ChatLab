/**
 * Agent API routes (server-side)
 * POST /api/agent/run — starts agent with SSE streaming response
 * POST /api/agent/abort/:requestId — aborts a running agent
 */

import { Router } from 'express'
import type { Request, Response } from 'express'
import { runAgentStream } from '../ai/agent/index.js'
import { addMessage } from '../ai/conversations.js'
import type { ToolContext } from '../ai/tools/types.js'
import type { AgentStreamChunk } from '../ai/agent/types.js'
import { BUILTIN_DESENSITIZE_RULES } from '../ai/preprocessor/builtin-rules.js'
import type { DesensitizeRule } from '../ai/preprocessor/types.js'

const router = Router()

// In-flight abort controllers keyed by requestId
const abortControllers = new Map<string, AbortController>()

/**
 * POST /api/agent/run
 *
 * Request body:
 *   message: string          — user message
 *   sessionId: string        — database session ID
 *   conversationId?: string  — AI conversation ID (for history)
 *   chatType?: 'group' | 'private'
 *   locale?: string
 *   requestId?: string       — client-generated ID for abort support
 *   maxToolRounds?: number
 *   contextHistoryLimit?: number
 *   timeFilter?: { startTs: number; endTs: number }
 *   maxMessagesLimit?: number
 *   ownerInfo?: { platformId: string; displayName: string }
 *   promptConfig?: { roleDefinition: string; responseRules: string }
 *   preprocessConfig?: object
 *
 * Response: SSE stream of AgentStreamChunk JSON lines
 */
router.post('/run', async (req: Request, res: Response) => {
  const {
    message,
    sessionId,
    conversationId,
    chatType = 'group',
    locale = 'zh-CN',
    requestId,
    maxToolRounds,
    contextHistoryLimit,
    timeFilter,
    maxMessagesLimit,
    ownerInfo,
    promptConfig,
    preprocessConfig,
  } = req.body

  if (!message || typeof message !== 'string') {
    res.status(400).json({ error: 'message is required' })
    return
  }

  if (!sessionId || typeof sessionId !== 'string') {
    res.status(400).json({ error: 'sessionId is required' })
    return
  }

  // SSE headers
  res.setHeader('Content-Type', 'text/event-stream')
  res.setHeader('Cache-Control', 'no-cache')
  res.setHeader('Connection', 'keep-alive')
  res.setHeader('X-Accel-Buffering', 'no')
  res.flushHeaders()

  // Abort controller
  const abortController = new AbortController()
  const rid = requestId || `req_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`
  abortControllers.set(rid, abortController)

  // Send requestId to client so it can abort later
  const sendSSE = (chunk: AgentStreamChunk) => {
    if (res.writableEnded) return
    res.write(`data: ${JSON.stringify(chunk)}\n\n`)
  }

  // Send the request ID
  res.write(`data: ${JSON.stringify({ type: 'meta', requestId: rid })}\n\n`)

  const context: ToolContext = {
    sessionId,
    conversationId,
    timeFilter,
    maxMessagesLimit,
    ownerInfo,
    locale,
    preprocessConfig,
  }

  try {
    const result = await runAgentStream(
      message,
      context,
      sendSSE,
      {
        maxToolRounds,
        contextHistoryLimit,
        abortSignal: abortController.signal,
      },
      chatType,
    )

    // Save assistant message to conversation if conversationId provided
    if (conversationId && result.content) {
      try {
        addMessage(conversationId, 'assistant', result.content)
      } catch (e) {
        // Non-fatal: log but don't fail the response
        console.error('Failed to save assistant message:', e)
      }
    }
  } catch (error) {
    if (!abortController.signal.aborted) {
      const errorMessage = error instanceof Error ? error.message : String(error)
      sendSSE({ type: 'error', error: errorMessage })
    }
  } finally {
    abortControllers.delete(rid)
    if (!res.writableEnded) {
      res.end()
    }
  }
})

/**
 * POST /api/agent/abort/:requestId
 * Aborts a running agent request.
 */
router.post('/abort/:requestId', (req: Request, res: Response) => {
  const requestId = Array.isArray(req.params.requestId) ? req.params.requestId[0] : req.params.requestId
  const controller = abortControllers.get(requestId)

  if (!controller) {
    res.status(404).json({ error: 'Request not found or already completed' })
    return
  }

  controller.abort()
  abortControllers.delete(requestId)
  res.json({ success: true })
})

/**
 * GET /api/agent/desensitize-rules
 * Returns built-in desensitize rules filtered by locale.
 * Query: locale (default: 'en')
 */
router.get('/desensitize-rules', (req: Request, res: Response) => {
  const locale = (req.query.locale as string) || 'en'
  const rules = BUILTIN_DESENSITIZE_RULES.filter(
    (r) => !r.locales || r.locales.length === 0 || r.locales.includes(locale)
  )
  res.json(rules)
})

/**
 * POST /api/agent/merge-desensitize-rules
 * Merges existing rules with built-in rules for a locale.
 * Body: { existingRules: DesensitizeRule[], locale: string }
 */
router.post('/merge-desensitize-rules', (req: Request, res: Response) => {
  const { existingRules = [], locale = 'en' } = req.body as {
    existingRules: DesensitizeRule[]
    locale: string
  }

  const builtinForLocale = BUILTIN_DESENSITIZE_RULES.filter(
    (r) => !r.locales || r.locales.length === 0 || r.locales.includes(locale)
  )

  // Start with existing rules, add any missing built-in rules
  const existingIds = new Set(existingRules.map((r) => r.id))
  const merged = [
    ...existingRules,
    ...builtinForLocale.filter((r) => !existingIds.has(r.id)),
  ]

  res.json(merged)
})

export default router
