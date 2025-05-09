// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import CartIcon from '../CartIcon';
import CurrencySwitcher from '../CurrencySwitcher';
import * as S from './Header.styled';

const IMAGE_BASE_URL =
  typeof window !== 'undefined' && window.ENV?.IMAGE_BASE_URL
    ? window.ENV.IMAGE_BASE_URL
    : process.env.IMAGE_BASE_URL;


const Header = () => {
  return (
    <S.Header>
      <S.NavBar>
        <S.Container>
          <S.NavBarBrand href="/">
            <S.BrandImg baseUrl={IMAGE_BASE_URL || 'https://oteldemo.s3.eu-west-3.amazonaws.com'} />
          </S.NavBarBrand>
          <S.Controls>
            <CurrencySwitcher />
            <CartIcon />
          </S.Controls>
        </S.Container>
      </S.NavBar>
    </S.Header>
  );
};

export default Header;
