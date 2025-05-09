// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { InputHTMLAttributes } from 'react';
import * as S from './Select.styled';

interface IProps extends InputHTMLAttributes<HTMLSelectElement> {
  children: React.ReactNode;
}

const IMAGE_BASE_URL =
  typeof window !== 'undefined' && window.ENV?.IMAGE_BASE_URL
    ? window.ENV.IMAGE_BASE_URL
    : process.env.IMAGE_BASE_URL;

const Select = ({ children, ...props }: IProps) => {
  return (
    <S.SelectContainer>
      <S.Select {...props}>{children}</S.Select>
      <S.Arrow baseUrl={IMAGE_BASE_URL || ''}/>
    </S.SelectContainer>
  );
};

export default Select;
