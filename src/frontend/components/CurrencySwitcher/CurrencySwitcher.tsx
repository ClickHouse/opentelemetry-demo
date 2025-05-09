// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { useMemo } from 'react';
import getSymbolFromCurrency from 'currency-symbol-map';
import { useCurrency } from '../../providers/Currency.provider';
import * as S from './CurrencySwitcher.styled';
import { CypressFields } from '../../utils/Cypress';

const IMAGE_BASE_URL =
  typeof window !== 'undefined' && window.ENV?.IMAGE_BASE_URL
    ? window.ENV.IMAGE_BASE_URL
    : process.env.IMAGE_BASE_URL;

const CurrencySwitcher = () => {
  const { currencyCodeList, setSelectedCurrency, selectedCurrency } = useCurrency();

  const currencySymbol = useMemo(() => getSymbolFromCurrency(selectedCurrency), [selectedCurrency]);

  return (
    <S.CurrencySwitcher>
      <S.Container>
        <S.SelectedConcurrency>{currencySymbol}</S.SelectedConcurrency>
        <S.Select
          name="currency_code"
          onChange={(event: { target: { value: string; }; }) => setSelectedCurrency(event.target.value)}
          value={selectedCurrency}
          data-cy={CypressFields.CurrencySwitcher}
        >
          {currencyCodeList.map(currencyCode => (
            <option key={currencyCode} value={currencyCode}>
              {currencyCode}
            </option>
          ))}
        </S.Select>
        <S.Arrow baseUrl={IMAGE_BASE_URL || ''} />
      </S.Container>
    </S.CurrencySwitcher>
  );
};

export default CurrencySwitcher;
