import type { WorkflowDefinition } from "./types";

export const PRESETS: { id: string; label: string; description: string; definition: WorkflowDefinition }[] = [
  {
    id: "retail",
    label: "Retail fulfillment",
    description: "Payment retry, fraud approval, parallel packing, durable timer.",
    definition: {
      name: "retail-order-fulfillment",
      version: "1",
      steps: [
        { id: "reserveInventory", action: "ledger", compensate: "ledger", value: { sku: "BOOK-001", quantity: 1 } },
        {
          id: "authorizePayment",
          action: "ledger",
          dependsOn: ["reserveInventory"],
          compensate: "ledger",
          failUntil: 1,
          backoffMs: 250,
          value: { amountCents: 2599 },
        },
        { id: "fraudReview", action: "approval", dependsOn: ["authorizePayment"] },
        { id: "packEast", action: "sleep", durationMs: 700, dependsOn: ["fraudReview"] },
        { id: "packWest", action: "sleep", durationMs: 900, dependsOn: ["fraudReview"] },
        { id: "dispatch", action: "wait", delayMs: 1000, dependsOn: ["packEast", "packWest"] },
        { id: "notifyCustomer", action: "echo", dependsOn: ["dispatch"], value: { notification: "Order dispatched" } },
      ],
    },
  },
  {
    id: "saga",
    label: "Compensation demo",
    description: "Shipping fails permanently; payment and inventory are undone in reverse order.",
    definition: {
      name: "fulfillment-compensation",
      version: "1",
      steps: [
        { id: "reserve", action: "ledger", compensate: "ledger", value: { inventory: "reserved" } },
        { id: "charge", action: "ledger", compensate: "ledger", dependsOn: ["reserve"], value: { payment: "authorized" } },
        { id: "ship", action: "fail", maxAttempts: 2, backoffMs: 100, dependsOn: ["charge"] },
      ],
    },
  },
  {
    id: "lease",
    label: "Long activity",
    description: "A 5-second activity that outlives one lease; heartbeats keep it on attempt 1.",
    definition: {
      name: "lease-renewal",
      version: "1",
      steps: [{ id: "longActivity", action: "sleep", durationMs: 5000 }],
    },
  },
];
