# Cricket Webhook Documentation

## Overview

The Cricket webhook sends real-time notifications for cricket match events.

When specific events occur during a match, we will send a POST request to your configured endpoint.

## Request Details

- **Method**: POST
- **Content-Type**: application/json
- **Webhook Type**: `cricket`

## Webhook Payload Structure

### Base Structure

```json
{
  "type": "cricket",
  "event": {
    "type": "<event_type>"
    // Event-specific properties
  }
}
```

## Cricket Event Types

The Cricket webhook supports the following event types:

### Appeal Event (`appeal`)

Triggered when an appeal is made during the match.

**Type Definition:**

```typescript
{
  type: "cricket";
  event: {
    type: "appeal";
    providerMatchId: string;
    inningsNumber: number;
    overNumber: number;
    ballNumber: number;
    ballTimestamp: number;
    batterName: string | null;
    nonStrikeBatterName: string | null;
    bowlerName: string | null;
    appealType: string;
    fielderName: string | null;
    batterRunsTotal: number | null;
    battingTeamName: string | null;
    bowlingTeamName: string | null;
  }
}
```

**Example:**

```json
{
  "type": "cricket",
  "event": {
    "type": "appeal",
    "providerMatchId": "12345abc",
    "inningsNumber": 1,
    "overNumber": 10,
    "ballNumber": 3,
    "ballTimestamp": 1706745600,
    "batterName": "John Smith",
    "nonStrikeBatterName": "Jane Doe",
    "bowlerName": "Bob Johnson",
    "appealType": "Caught",
    "fielderName": "Job Bobson",
    "batterRunsTotal": 45,
    "bowlingTeamName": "Queensland",
    "battingTeamName": "South Australia Men"
  }
}
```

### Half Century Event (`halfCentury`)

Triggered when a batter scores 50 runs.

**Type Definition:**

```typescript
{
  type: "cricket";
  event: {
    type: "halfCentury";
    providerMatchId: string;
    inningsNumber: number;
    overNumber: number;
    ballNumber: number;
    batterName: string;
    batterRunsTotal: number;
    battingTeamName: string;
    bowlingTeamName: string;
  }
}
```

**Example:**

```json
{
  "type": "cricket",
  "event": {
    "type": "halfCentury",
    "providerMatchId": "12345abc",
    "inningsNumber": 1,
    "overNumber": 18,
    "ballNumber": 3,
    "batterName": "John Smith",
    "batterRunsTotal": 53,
    "bowlingTeamName": "Queensland",
    "battingTeamName": "South Australia Men"
  }
}
```

### Century Event (`century`)

Triggered when a batter scores 100 runs.

**Type Definition:**

```typescript
{
  type: "cricket";
  event: {
    type: "century";
    providerMatchId: string;
    inningsNumber: number;
    overNumber: number;
    ballNumber: number;
    batterName: string;
    batterRunsTotal: number;
    battingTeamName: string;
    bowlingTeamName: string;
  }
}
```

**Example:**

```json
{
  "type": "cricket",
  "event": {
    "type": "century",
    "providerMatchId": "12345abc",
    "inningsNumber": 1,
    "overNumber": 35,
    "ballNumber": 4,
    "batterName": "John Smith",
    "batterRunsTotal": 105,
    "bowlingTeamName": "Queensland",
    "battingTeamName": "South Australia Men"
  }
}
```

### Dismissal Event (`dismissal`)

Triggered when a batter is dismissed.

**Type Definition:**

```typescript
{
  type: "cricket";
  event: {
    type: "dismissal";
    providerMatchId: string;
    inningsNumber: number;
    overNumber: number;
    ballNumber: number;
    ballTimestamp: number;
    batterName: string | null;
    nonStrikeBatterName: string | null;
    bowlerName: string | null;
    dismissalType: string;
    fielderName: string | null;
    batterRunsTotal: number | null;
    battingTeamName: string | null;
    bowlingTeamName: string | null;
  }
}
```

**Example:**

```json
{
  "type": "cricket",
  "event": {
    "type": "dismissal",
    "providerMatchId": "12345abc",
    "inningsNumber": 1,
    "overNumber": 15,
    "ballNumber": 2,
    "ballTimestamp": 1706745600,
    "batterName": "John Smith",
    "nonStrikeBatterName": "Jane Doe",
    "bowlerName": "Bob Johnson",
    "dismissalType": "Caught",
    "fielderName": "Job Bobson",
    "batterRunsTotal": 67,
    "bowlingTeamName": "Queensland",
    "battingTeamName": "South Australia Men"
  }
}
```

### Dropped Catch Event (`droppedCatch`)

Triggered when a fielder drops a catch.

**Type Definition:**

```typescript
{
  type: "cricket";
  event: {
    type: "droppedCatch";
    providerMatchId: string;
    inningsNumber: number;
    overNumber: number;
    ballNumber: number;
    ballTimestamp: number;
    batterName: string | null;
    nonStrikeBatterName: string | null;
    bowlerName: string | null;
    fielderName: string | null;
    fieldPosition: string | null;
    batterRunsTotal: number | null;
    battingTeamName: string | null;
    bowlingTeamName: string | null;
  }
}
```

**Example:**

```json
{
  "type": "cricket",
  "event": {
    "type": "droppedCatch",
    "providerMatchId": "12345abc",
    "inningsNumber": 1,
    "overNumber": 20,
    "ballNumber": 5,
    "ballTimestamp": 1706745600,
    "batterName": "John Smith",
    "nonStrikeBatterName": "Jane Doe",
    "bowlerName": "Bob Johnson",
    "fielderName": "Job Bobson",
    "fieldPosition": "Point",
    "batterRunsTotal": 34,
    "bowlingTeamName": "Queensland",
    "battingTeamName": "South Australia Men"
  }
}
```

### First Ball Innings Event (`firstBallInnings`)

Triggered at the start of an innings.

**Type Definition:**

```typescript
{
  type: "cricket";
  event: {
    type: "firstBallInnings";
    providerMatchId: string;
    inningsNumber: number;
    battingTeamName: string | null;
    bowlingTeamName: string | null;
  }
}
```

**Example:**

```json
{
  "type": "cricket",
  "event": {
    "type": "firstBallInnings",
    "providerMatchId": "12345abc",
    "inningsNumber": 1,
    "bowlingTeamName": "Queensland",
    "battingTeamName": "South Australia Men"
  }
}
```

### Boundary Event (`boundary`)

Triggered when a boundary (4 or 6) is scored.

**Type Definition:**

```typescript
{
  type: "cricket";
  event: {
    type: "boundary";
    providerMatchId: string;
    inningsNumber: number;
    overNumber: number;
    ballNumber: number;
    ballTimestamp: number;
    batterName: string | null;
    nonStrikeBatterName: string | null;
    bowlerName: string | null;
    batterRunsScored: 4 | 6;
    shotType: string | null;
    fieldPosition: string | null;
    battingTeamName: string | null;
    bowlingTeamName: string | null;
  }
}
```

**Example:**

```json
{
  "type": "cricket",
  "event": {
    "type": "boundary",
    "providerMatchId": "12345abc",
    "inningsNumber": 1,
    "overNumber": 12,
    "ballNumber": 4,
    "ballTimestamp": 1706745600,
    "batterName": "John Smith",
    "nonStrikeBatterName": "Jane Doe",
    "bowlerName": "Bob Johnson",
    "batterRunsScored": 4,
    "shotType": "Drive",
    "fieldPosition": "Cover",
    "bowlingTeamName": "Queensland",
    "battingTeamName": "South Australia Men",
  }
}
```

## Field Descriptions

### Common Fields

- **type**: The webhook trigger type, always `"cricket"` for cricket events
- **event.type**: The specific cricket event type
- **providerMatchId**: Unique identifier for the match from the data provider
- **battingTeamName**: Name of the current batting team (nullable)
- **bowlingTeamName**: Name of the current bowling team (nullable)

### Ball Event Fields (for appeal, dismissal, droppedCatch, boundary)

- **inningsNumber**: The current innings number (1, 2, 3, etc. Innings value will keep increasing with more tied super overs.)
- **overNumber**: The current over number
- **ballNumber**: The ball number within the current over (1-6)
- **ballTimestamp**: Unix timestamp (seconds) when the ball was bowled
- **batterName**: Name of the batter on strike (nullable)
- **nonStrikeBatterName**: Name of the non-striking batter (nullable)
- **bowlerName**: Name of the bowler (nullable)

### Event-Specific Fields

#### Appeal Event

- **appealType**: Type of appeal (e.g., "Bowled", "Caught", "L.B.W.", "Run Out")
- **fielderName**: Name of the fielder making the appeal (nullable)
- **batterRunsTotal**: Total runs scored by the batter (nullable)

#### Century/Half Century Events

- **batterRunsTotal**: Total runs scored by the batter

#### Dismissal Event

- **dismissalType**: Type of dismissal (e.g., "Bowled", "Caught", "LBW", "Run out")
- **fielderName**: Name of the fielder involved in the dismissal (nullable)
- **batterRunsTotal**: Total runs scored by the dismissed batter (nullable)

#### Dropped Catch Event

- **fielderName**: Name of the fielder who dropped the catch (nullable)
- **fieldPosition**: Field position where the catch was dropped (nullable)
- **batterRunsTotal**: Total runs scored by the batter (nullable)

#### Boundary Event

- **batterRunsScored**: Number of runs scored (4 or 6)
- **shotType**: Type of shot played (nullable)
- **fieldPosition**: Field position where the ball crossed the boundary (nullable)

## Important Notes

1. All nullable fields may be `null` depending on the data availability
2. The `ballTimestamp` is provided in Unix seconds format
3. Event types are case-sensitive and should match exactly as documented

