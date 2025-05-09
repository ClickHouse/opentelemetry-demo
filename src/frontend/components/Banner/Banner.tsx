// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import Link from 'next/link';
import * as S from './Banner.styled';

const IMAGE_BASE_URL =
  typeof window !== 'undefined' && window.ENV?.IMAGE_BASE_URL
    ? window.ENV.IMAGE_BASE_URL
    : process.env.IMAGE_BASE_URL;

const Banner = () => {
  return (
    <S.Banner>
      <S.ImageContainer>
        <S.BannerImg baseUrl={IMAGE_BASE_URL || 'https://oteldemo.s3.eu-west-3.amazonaws.com'}/>
      </S.ImageContainer>
      <S.TextContainer>
        <S.Title>The best telescopes to see the world closer</S.Title>
        <Link href="#hot-products"><S.GoShoppingButton>Go Shopping</S.GoShoppingButton></Link>
      </S.TextContainer>
    </S.Banner>
  );
};

export default Banner;
