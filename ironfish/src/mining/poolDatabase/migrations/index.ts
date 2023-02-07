/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import Migration001 from './001-initial'
import Migration002 from './002-add-shares-index'
import Migration003 from './003-add-transaction-hash'
import Migration004 from './004-add-shares-address-index'
import Migration005 from './005-add-payout-period-table'

export const MIGRATIONS = [Migration001, Migration002, Migration003, Migration004, Migration005]
