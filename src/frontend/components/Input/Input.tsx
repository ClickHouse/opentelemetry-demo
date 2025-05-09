// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { HTMLInputTypeAttribute, InputHTMLAttributes } from 'react';
import * as S from './Input.styled';

interface IProps extends InputHTMLAttributes<HTMLSelectElement | HTMLInputElement> {
  type: HTMLInputTypeAttribute | 'select';
  children?: React.ReactNode;
  label: string;
}

const IMAGE_BASE_URL =
  typeof window !== 'undefined' && window.ENV?.IMAGE_BASE_URL
    ? window.ENV.IMAGE_BASE_URL
    : process.env.IMAGE_BASE_URL;

const Input = ({ type, id = '', children, label, ...props }: IProps) => {
  return (
    <S.InputRow>
      <S.InputLabel>{label}</S.InputLabel>
      {type === 'select' ? (
        <>
          <S.Select id={id} {...props}>
            {children}
          </S.Select>
          <S.Arrow baseUrl={IMAGE_BASE_URL || ''} />
        </>
      ) : (
        <S.Input id={id} {...props} type={type} />
      )}
    </S.InputRow>
  );
};

export default Input;
