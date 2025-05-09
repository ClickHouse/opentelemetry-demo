// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import { CypressFields } from '../../utils/Cypress';
import { Product } from '../../protos/demo';
import ProductPrice from '../ProductPrice';
import * as S from './ProductCard.styled';
// import { useState, useEffect } from 'react';
// import { useNumberFlagValue } from '@openfeature/react-sdk';

interface IProps {
  product: Product;
}

// async function getImageWithHeaders(requestInfo: Request) {
//   const res = await fetch(requestInfo);
//   return await res.blob();
// }

const {
  IMAGE_BASE_URL
} = typeof window !== 'undefined' ? window.ENV : {};

const ProductCard = ({
  product: {
    id,
    picture,
    name,
    priceUsd = {
      currencyCode: 'USD',
      units: 0,
      nanos: 0,
    },
  },
}: IProps) => {
  // const imageSlowLoad = useNumberFlagValue('imageSlowLoad', 0);
  // const [imageSrc, setImageSrc] = useState<string>('');

  // useEffect(() => {
  //   const headers = new Headers();
  //   headers.append('x-envoy-fault-delay-request', imageSlowLoad.toString());
  //   headers.append('Cache-Control', 'no-cache')
  //   const requestInit = {
  //     method: "GET",
  //     headers: headers
  //   };
  //   const image_url =`${IMAGE_BASE_URL}/images/products/` + picture
  //   const requestInfo = new Request(image_url, requestInit);
  //   setImageSrc(image_url);
  //   getImageWithHeaders(requestInfo).then(blob => {
  //     setImageSrc(URL.createObjectURL(blob));
  //   });
  // }, [imageSlowLoad, picture]);

  return (
    <S.Link href={`/product/${id}`}>
      <S.ProductCard data-cy={CypressFields.ProductCard}>
        <S.Image $src={`${IMAGE_BASE_URL}/images/products/` + picture} />
        <div>
          <S.ProductName>{name}</S.ProductName>
          <S.ProductPrice>
            <ProductPrice price={priceUsd} />
          </S.ProductPrice>
        </div>
      </S.ProductCard>
    </S.Link>
  );
};

export default ProductCard;
