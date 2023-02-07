/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import { Assert } from '../assert'
import { createRootLogger } from '../logger'
import { createRouteTest } from '../testUtilities/routeTest'
import { MiningPoolShares } from './poolShares'

describe('poolShares', () => {
  const routeTest = createRouteTest()
  let shares: MiningPoolShares

  beforeEach(async () => {
    shares = await MiningPoolShares.init({
      rpc: routeTest.client,
      config: routeTest.sdk.config,
      logger: createRootLogger().withTag('test'),
      enablePayouts: false,
      dbPath: ':memory:',
    })

    await shares.start()
  })

  afterEach(async () => {
    await shares.stop()
  })

  it('rolloverPayoutPeriod', async () => {
    jest.useFakeTimers({ legacyFakeTimers: false })

    const now = new Date(2020, 1, 1).getTime()
    jest.setSystemTime(now)

    const payoutPeriod0 = await shares['db'].getCurrentPayoutPeriod()
    expect(payoutPeriod0).toBeUndefined()

    await shares.rolloverPayoutPeriod()

    const payoutPeriod1 = await shares['db'].getCurrentPayoutPeriod()
    Assert.isNotUndefined(payoutPeriod1, 'payoutPeriod1 should exist')

    // No time has elapsed, so it will not rollover
    await shares.rolloverPayoutPeriod()

    const payoutPeriod1A = await shares['db'].getCurrentPayoutPeriod()
    expect(payoutPeriod1A).toEqual(payoutPeriod1)

    // Move the clock forward the amount of time needed to trigger a new payout rollover
    jest.setSystemTime(now + shares.config.get('poolPayoutPeriodDuration') * 1000)

    await shares.rolloverPayoutPeriod()

    const payoutPeriod2 = await shares['db'].getCurrentPayoutPeriod()
    Assert.isNotUndefined(payoutPeriod2, 'payoutPeriod2 should exist')
    expect(payoutPeriod2.id).toEqual(payoutPeriod1.id + 1)

    jest.useRealTimers()
  })

  it('blocks', async () => {
    await shares.rolloverPayoutPeriod()

    const reward = BigInt(200000)
    await shares.submitBlock(1, 'hash1', reward)
    await shares.submitBlock(2, 'hash2', reward * BigInt(-1))

    const unconfirmedBlocks1 = await shares.unconfirmedBlocks()
    expect(unconfirmedBlocks1.length).toEqual(2)

    // This should be a no-op
    await shares.updateBlockStatus(unconfirmedBlocks1[0], true, false)

    const unconfirmedBlocks2 = await shares.unconfirmedBlocks()
    expect(unconfirmedBlocks2.length).toEqual(2)

    // This should update the 'main' boolean, but the block should still show up
    await shares.updateBlockStatus(unconfirmedBlocks2[0], false, false)

    const unconfirmedBlocks3 = await shares.unconfirmedBlocks()
    expect(unconfirmedBlocks3.length).toEqual(2)
    expect(unconfirmedBlocks3[0]).toMatchObject({
      blockSequence: 1,
      blockHash: 'hash1',
      minerReward: reward,
      main: false,
    })

    await shares.updateBlockStatus(unconfirmedBlocks3[0], false, true)

    const unconfirmedBlocks4 = await shares.unconfirmedBlocks()
    expect(unconfirmedBlocks4.length).toEqual(1)
    expect(unconfirmedBlocks4[0]).toMatchObject({
      blockSequence: 2,
      blockHash: 'hash2',
      minerReward: reward,
    })
  })
})
