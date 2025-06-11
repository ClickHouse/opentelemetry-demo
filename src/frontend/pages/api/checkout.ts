// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

import type { NextApiRequest, NextApiResponse } from 'next';
import CheckoutGateway from '../../gateways/rpc/Checkout.gateway';
import { Empty, PlaceOrderRequest } from '../../protos/demo';
import { IProductCheckoutItem, IProductCheckout } from '../../types/Cart';
import ProductCatalogService from '../../services/ProductCatalog.service';
import * as HyperDX from '@hyperdx/node-opentelemetry';
import logger from '../../utils/logger'; // ✅ Import the logger

type TResponse = IProductCheckout | Empty;

const handler = async ({ method, body, query }: NextApiRequest, res: NextApiResponse<TResponse>) => {
  switch (method) {
    case 'POST': {
      try {
        
        const { currencyCode = '' } = query;
        logger.info("Checkout", { currencyCode: currencyCode });
        const orderData = body as PlaceOrderRequest;
        HyperDX.setTraceAttributes({
          userId: orderData.userId,
          email: orderData.email,
        });
        const { order: { items = [], ...order } = {} } = await CheckoutGateway.placeOrder(orderData);

        const productList: IProductCheckoutItem[] = await Promise.all(
          items.map(async ({ item: { productId = '', quantity = 0 } = {}, cost }) => {
            const product = await ProductCatalogService.getProduct(productId, currencyCode as string);

            return {
              cost,
              item: {
                productId,
                quantity,
                product,
              },
            };
          })
        );

        return res.status(200).json({ ...order, items: productList });
      } catch (err) {
        // console.error('Failed to place order', { error: err });
        logger.error('Failed to place order', { error: err }); 
        throw err; // Rethrow the error to be caught by the catch block
        return res.status(500).json({ message: 'Internal Server Error' });
      }
    }

    default: {
      return res.status(405).send('');
    }
  }
};

export default handler;
