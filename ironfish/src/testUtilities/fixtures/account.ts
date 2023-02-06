/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
import { Account, AccountValue, Wallet } from '../../wallet'
import { WithRequired } from '../types'
import { FixtureGenerate, useFixture } from './fixture'

export function useAccountFixture(
  wallet: Wallet,
  generate: FixtureGenerate<WithRequired<Account, 'spendingKey'>> | string = 'test',
  setDefault = false,
): Promise<WithRequired<Account, 'spendingKey'>> {
  if (typeof generate === 'string') {
    const name = generate
    generate = async () => {
      const account = await wallet.createAccount(name, setDefault)
      return account as WithRequired<Account, 'spendingKey'>
    }
  }

  return useFixture(generate, {
    serialize: (
      account: WithRequired<Account, 'spendingKey'>,
    ): WithRequired<AccountValue, 'spendingKey'> => {
      return account.serialize() as WithRequired<AccountValue, 'spendingKey'>
    },

    deserialize: async (
      accountData: WithRequired<AccountValue, 'spendingKey'>,
    ): Promise<WithRequired<Account, 'spendingKey'>> => {
      const account = await wallet.importAccount(accountData)
      if (wallet.chainProcessor.hash && wallet.chainProcessor.sequence) {
        await account.updateHead({
          hash: wallet.chainProcessor.hash,
          sequence: wallet.chainProcessor.sequence,
        })
      } else {
        await account.updateHead(null)
      }
      return account as WithRequired<Account, 'spendingKey'>
    },
  })
}
