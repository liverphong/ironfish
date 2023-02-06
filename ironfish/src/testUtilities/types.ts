/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

// When used, this type will require a value be set and non-null
// ie Account.spendingKey? is optional
// with WithRequired<Account, 'spendingKey'>, the return type has Account.spendingKey (non optional)
export type WithRequired<T, K extends keyof T> = T & { [P in K]-?: T[P] }
