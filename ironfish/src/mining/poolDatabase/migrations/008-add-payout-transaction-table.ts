/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import { Database } from 'sqlite'
import { Migration } from '../migration'

export default class Migration008 extends Migration {
  name = '008-add-payout-transaction-table'

  async forward(db: Database): Promise<void> {
    await db.run(`
      CREATE TABLE payoutTransaction (
        id INTEGER PRIMARY KEY,
        createdAt INTEGER NOT NULL DEFAULT CURRENT_TIMESTAMP,
        transactionHash TEXT NOT NULL,
        confirmed BOOLEAN DEFAULT FALSE,
        expired BOOLEAN DEFAULT FALSE,
        payoutPeriodId INTEGER NOT NULL,
        CONSTRAINT payoutTransaction_fk_payoutPeriodId FOREIGN KEY (payoutPeriodId) REFERENCES payoutPeriod (id) 
      );
     `)

    await db.run(`
      ALTER TABLE payoutShare ADD payoutTransactionId INTEGER REFERENCES payoutPeriodId (id);
   `)
  }

  async backward(db: Database): Promise<void> {
    await db.run(`ALTER TABLE payoutShare DROP COLUMN payoutTransactionId;`)
    await db.run('DROP TABLE IF EXISTS payoutTransaction;')
  }
}
